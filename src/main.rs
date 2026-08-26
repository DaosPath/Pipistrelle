mod bridge;
mod codec;
mod config;
mod latency;
mod metrics;
mod persistence;
mod router;
mod session;
mod tls;
mod websocket;

use bytes::{Buf, BytesMut};
use std::fs::File;
use std::io::{self, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{Level, debug, error, info, warn};
use tracing_subscriber::FmtSubscriber;

use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

use crate::codec::{
    ConnAck, Packet, PubAck, SubAck, UnsubAck, decode_packet_limited, encode_packet,
};
use crate::router::topic_filter_valid;
use crate::session::{
    ApplicationProperties, BrokerState, ClientSession, IncomingQos2Message, OutboundPacket,
    OutgoingQos2Phase, WillMessage,
};
use pipistrelle::{crypto, version};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Keep benchmark/production logging cheap by default; DEBUG/TRACE are opt-in.
    let log_level = match std::env::var("PIPISTRELLE_LOG_LEVEL")
        .unwrap_or_else(|_| "info".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "error" => Level::ERROR,
        "warn" => Level::WARN,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::INFO,
    };
    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!(
        "Starting Pipistrelle {} {} MQTT v5.0 Broker...",
        version::SERIES,
        version::VERSION
    );

    // Read environment variables for port overrides
    let port_tcp = std::env::var("PIPISTRELLE_PORT_TCP")
        .unwrap_or_else(|_| "1883".to_string())
        .parse::<u16>()
        .unwrap_or(1883);

    let port_tls = std::env::var("PIPISTRELLE_PORT_TLS")
        .unwrap_or_else(|_| "8883".to_string())
        .parse::<u16>()
        .unwrap_or(8883);

    let port_ws = std::env::var("PIPISTRELLE_PORT_WS")
        .unwrap_or_else(|_| "8083".to_string())
        .parse::<u16>()
        .unwrap_or(8083);

    let port_metrics = std::env::var("PIPISTRELLE_PORT_METRICS")
        .unwrap_or_else(|_| "9090".to_string())
        .parse::<u16>()
        .unwrap_or(9090);

    // Parse CLI arguments
    let mut cert_path = None;
    let mut key_path = None;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--cert" => {
                if i + 1 < args.len() {
                    cert_path = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
                } else {
                    error!("Missing value for --cert");
                    return Err("Invalid arguments".into());
                }
            }
            "--key" => {
                if i + 1 < args.len() {
                    key_path = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
                } else {
                    error!("Missing value for --key");
                    return Err("Invalid arguments".into());
                }
            }
            _ => {
                warn!("Unknown argument: {}", args[i]);
                i += 1;
            }
        }
    }

    // Default paths if not specified
    let cert_file = cert_path.unwrap_or_else(|| PathBuf::from("cert.pem"));
    let key_file = key_path.unwrap_or_else(|| PathBuf::from("key.pem"));

    // Ensure certificates exist (autogenerate if missing)
    if let Err(e) = tls::ensure_certificates(&cert_file, &key_file) {
        error!("Failed to ensure TLS certificates: {:?}", e);
    }

    // Check if certificates exist and load TLS acceptor
    let tls_acceptor = if cert_file.exists() && key_file.exists() {
        info!("Loading TLS certificates from: {:?}", cert_file);
        info!("Loading TLS private key from: {:?}", key_file);
        match (load_certs(&cert_file), load_key(&key_file)) {
            (Ok(certs), Ok(key)) => {
                let tls_profile = crypto::TlsProfile::from_env(
                    "PIPISTRELLE_TLS_PROFILE",
                    crypto::TlsProfile::Hybrid,
                );
                let provider = crypto::provider(tls_profile);
                let server_config = ServerConfig::builder_with_provider(Arc::new(provider))
                    .with_protocol_versions(&[&tokio_rustls::rustls::version::TLS13])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
                    .with_no_client_auth()
                    .with_single_cert(certs, key)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

                info!(
                    "TLS 1.3 crypto profile '{}' initialized: {}",
                    tls_profile,
                    tls_profile.description()
                );
                Some(TlsAcceptor::from(Arc::new(server_config)))
            }
            (Err(e), _) => {
                error!("Failed to load certificates: {:?}", e);
                None
            }
            (_, Err(e)) => {
                error!("Failed to load private key: {:?}", e);
                None
            }
        }
    } else {
        info!("TLS certificates not found. Running in plain TCP mode only.");
        None
    };

    let broker_state = Arc::new(BrokerState::new());

    // Restore persistent sessions and subscriptions from database on startup
    broker_state.restore_sessions_from_db().await;

    // Start Prometheus metrics exporter
    metrics::start_metrics_server(port_metrics, broker_state.clone()).await;

    // Start MQTT Bridging engine to HiveMQ Cloud
    bridge::start_bridge_engine(broker_state.clone()).await;

    // 1. Start plain TCP listener
    let plain_addr = format!("0.0.0.0:{}", port_tcp);
    let plain_listener = TcpListener::bind(&plain_addr).await?;
    info!("Plain TCP listening on: {}", plain_addr);

    let state_clone = broker_state.clone();
    tokio::spawn(async move {
        loop {
            match plain_listener.accept().await {
                Ok((socket, addr)) => {
                    let state = state_clone.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(socket, addr, state).await {
                            debug!("Plain TCP connection closed with error: {:?}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept plain connection: {:?}", e);
                }
            }
        }
    });

    // 2. Start TLS listener if enabled
    if let Some(acceptor) = tls_acceptor {
        let tls_addr = format!("0.0.0.0:{}", port_tls);
        let tls_listener = TcpListener::bind(&tls_addr).await?;
        info!("Secure TLS listening on: {}", tls_addr);

        let state_clone = broker_state.clone();
        tokio::spawn(async move {
            loop {
                match tls_listener.accept().await {
                    Ok((socket, addr)) => {
                        let state = state_clone.clone();
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            match acceptor.accept(socket).await {
                                Ok(tls_stream) => {
                                    if let Some(group) = tls_stream
                                        .get_ref()
                                        .1
                                        .negotiated_key_exchange_group()
                                        .map(|group| group.name())
                                    {
                                        let is_pqc = matches!(
                                            group,
                                            tokio_rustls::rustls::NamedGroup::X25519MLKEM768
                                                | tokio_rustls::rustls::NamedGroup::MLKEM768
                                                | tokio_rustls::rustls::NamedGroup::MLKEM1024
                                                | tokio_rustls::rustls::NamedGroup::secp256r1MLKEM768
                                        );
                                        if is_pqc {
                                            state
                                                .metrics_tls_pqc_handshakes
                                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        } else {
                                            state
                                                .metrics_tls_classical_handshakes
                                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        }
                                        debug!("TLS negotiated key exchange group: {:?}", group);
                                    }
                                    if let Err(e) = handle_connection(tls_stream, addr, state).await
                                    {
                                        debug!("TLS connection closed with error: {:?}", e);
                                    }
                                }
                                Err(e) => {
                                    warn!("TLS handshake failed for {}: {:?}", addr, e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept TLS connection: {:?}", e);
                    }
                }
            }
        });
    }

    // 3. Start WebSocket listener on port 8083
    let ws_addr = format!("0.0.0.0:{}", port_ws);
    let ws_listener = TcpListener::bind(&ws_addr).await?;
    info!("WebSocket TCP listening on: {}", ws_addr);

    let state_clone = broker_state.clone();
    tokio::spawn(async move {
        loop {
            match ws_listener.accept().await {
                Ok((socket, addr)) => {
                    let state = state_clone.clone();
                    tokio::spawn(async move {
                        match tokio_tungstenite::accept_async(socket).await {
                            Ok(ws_stream) => {
                                let adapter = websocket::WebSocketStreamAdapter::new(ws_stream);
                                if let Err(e) = handle_connection(adapter, addr, state).await {
                                    debug!("WebSocket connection closed with error: {:?}", e);
                                }
                            }
                            Err(e) => {
                                warn!("WebSocket handshake failed for {}: {:?}", addr, e);
                            }
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept WebSocket connection: {:?}", e);
                }
            }
        }
    });

    // Wait for Ctrl-C shutdown signal
    if let Err(e) = tokio::signal::ctrl_c().await {
        error!("Failed to register Ctrl-C shutdown handler: {:?}", e);
    }

    info!("Shutdown signal received. Initiating graceful shutdown...");
    broker_state.graceful_shutdown().await;

    // Let connection channels flush before exiting
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("Pipistrelle Broker shutdown complete.");
    Ok(())
}

async fn handle_connection<S>(
    mut socket: S,
    addr: SocketAddr,
    state: Arc<BrokerState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    debug!("New connection from: {}", addr);

    let mut read_buf = BytesMut::with_capacity(4096);

    // 1. Wait for the CONNECT packet first. TCP can fragment a Control Packet across
    // arbitrary reads, so accumulate until the entire CONNECT is available or the
    // five-second handshake deadline expires.
    let connect_deadline = Instant::now() + Duration::from_secs(5);
    let (
        mut client_id,
        keep_alive,
        clean_start,
        session_expiry_interval,
        client_receive_maximum,
        client_maximum_packet_size,
        username,
        password,
        will,
    ) = loop {
        match decode_packet_limited(&read_buf, state.maximum_packet_size) {
            Ok((Packet::Connect(pkt), bytes_read)) => {
                let client_id = pkt.client_id.to_string();
                let keep_alive = pkt.keep_alive;
                let clean_start = pkt.clean_start;
                let session_expiry_interval = pkt.properties.session_expiry_interval.unwrap_or(0);
                let client_receive_maximum = pkt.properties.receive_maximum.unwrap_or(u16::MAX);
                let client_maximum_packet_size =
                    pkt.properties.max_packet_size.unwrap_or(268_435_455);
                let username = pkt.username.map(|s| s.to_string());
                let password = pkt
                    .password
                    .map(|b| String::from_utf8_lossy(b).into_owned());
                let will = pkt.will.map(|will| WillMessage {
                    topic: will.topic.to_string(),
                    payload: will.payload.to_vec(),
                    qos: will.qos,
                    retain: will.retain,
                    delay_interval: will.properties.will_delay_interval.unwrap_or(0),
                    properties: ApplicationProperties::from_will(&will.properties),
                });
                read_buf.advance(bytes_read);
                break (
                    client_id,
                    keep_alive,
                    clean_start,
                    session_expiry_interval,
                    client_receive_maximum,
                    client_maximum_packet_size,
                    username,
                    password,
                    will,
                );
            }
            Ok((other, _)) => {
                warn!("First packet was not CONNECT: {:?}", other);
                return Ok(());
            }
            Err(codec::CodecError::Incomplete) => {
                let remaining = connect_deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    warn!("CONNECT handshake timeout from {}", addr);
                    return Ok(());
                }
                match tokio::time::timeout(remaining, socket.read_buf(&mut read_buf)).await {
                    Ok(Ok(n)) if n > 0 => continue,
                    _ => {
                        warn!("Timeout or connection closed before CONNECT completed");
                        return Ok(());
                    }
                }
            }
            Err(codec::CodecError::UnsupportedProtocolVersion(_)) => {
                let connack = Packet::ConnAck(ConnAck {
                    session_present: false,
                    reason_code: 0x84,
                    properties: Default::default(),
                });
                let mut buf = Vec::new();
                encode_packet(&connack, &mut buf);
                let _ = socket.write_all(&buf).await;
                let _ = socket.shutdown().await;
                return Ok(());
            }
            Err(codec::CodecError::PacketTooLarge) => {
                let connack = Packet::ConnAck(ConnAck {
                    session_present: false,
                    reason_code: 0x95,
                    properties: Default::default(),
                });
                let mut buf = Vec::new();
                encode_packet(&connack, &mut buf);
                let _ = socket.write_all(&buf).await;
                let _ = socket.shutdown().await;
                return Ok(());
            }
            Err(codec::CodecError::ProtocolError) => {
                let connack = Packet::ConnAck(ConnAck {
                    session_present: false,
                    reason_code: 0x82,
                    properties: Default::default(),
                });
                let mut buf = Vec::new();
                encode_packet(&connack, &mut buf);
                let _ = socket.write_all(&buf).await;
                let _ = socket.shutdown().await;
                return Ok(());
            }
            Err(e) => {
                warn!("Failed to decode CONNECT packet: {:?}", e);
                return Ok(());
            }
        }
    };

    // Authenticate client
    let authenticated = match (&username, &password) {
        (Some(u), Some(p)) => state.auth.authenticate(u, p).await,
        (None, None) => state.auth.authenticate("", "").await, // Try anonymous access
        _ => false, // Missing either username or password when the other is present
    };

    if !authenticated {
        warn!(
            "Authentication failed for client '{}' (username: {:?})",
            client_id, username
        );
        let connack = Packet::ConnAck(ConnAck {
            session_present: false,
            reason_code: 0x86, // Bad User Name or Password
            properties: Default::default(),
        });
        let mut connack_buf = Vec::new();
        encode_packet(&connack, &mut connack_buf);
        let _ = socket.write_all(&connack_buf).await;
        let _ = socket.shutdown().await;
        return Ok(());
    }

    let assigned_client_identifier = if client_id.is_empty() {
        client_id = state.allocate_client_id();
        Some(client_id.clone())
    } else {
        None
    };

    info!(
        "Client '{}' (authenticated user: {:?}) connecting from {}",
        client_id, username, addr
    );

    let existing = state.existing_session(&client_id);

    // Pipistrelle binds persistent Session state to the authenticated principal as
    // well as the MQTT ClientID. Without this guard, a different valid user who
    // guessed/reused a ClientID could inherit subscriptions and queued messages that
    // were authorized under another ACL identity.
    if existing
        .as_ref()
        .is_some_and(|session| session.username != username)
    {
        warn!(
            "Rejecting ClientID '{}' because it belongs to a different authenticated principal",
            client_id
        );
        let connack = Packet::ConnAck(ConnAck {
            session_present: false,
            reason_code: 0x87, // Not Authorized
            properties: Default::default(),
        });
        let mut connack_buf = Vec::new();
        encode_packet(&connack, &mut connack_buf);
        let _ = socket.write_all(&connack_buf).await;
        let _ = socket.shutdown().await;
        return Ok(());
    }

    if state
        .pending_will_principal(&client_id)
        .is_some_and(|owner| owner != username)
    {
        warn!(
            "Rejecting ClientID '{}' because its persisted Will belongs to a different authenticated principal",
            client_id
        );
        let connack = Packet::ConnAck(ConnAck {
            session_present: false,
            reason_code: 0x87,
            properties: Default::default(),
        });
        let mut connack_buf = Vec::new();
        encode_packet(&connack, &mut connack_buf);
        let _ = socket.write_all(&connack_buf).await;
        let _ = socket.shutdown().await;
        return Ok(());
    }

    let session_present = !clean_start
        && existing
            .as_ref()
            .is_some_and(|session| session.session_expiry() > 0);

    // Negotiate a CONNACK that respects the client's Maximum Packet Size before
    // mutating Session/Will ownership. Optional capability properties are removed
    // if needed; Assigned Client Identifier is mandatory when the CONNECT ClientID was empty.
    let mut connack_buf = Vec::new();
    let full_connack = Packet::ConnAck(ConnAck {
        session_present,
        reason_code: 0,
        properties: crate::codec::ConnAckProperties {
            receive_maximum: Some(state.receive_maximum),
            maximum_packet_size: Some(state.maximum_packet_size as u32),
            assigned_client_identifier: assigned_client_identifier.as_deref(),
            topic_alias_maximum: Some(0),
            ..Default::default()
        },
    });
    encode_packet(&full_connack, &mut connack_buf);
    if connack_buf.len() > client_maximum_packet_size as usize {
        connack_buf.clear();
        let minimal_connack = Packet::ConnAck(ConnAck {
            session_present,
            reason_code: 0,
            properties: crate::codec::ConnAckProperties {
                assigned_client_identifier: assigned_client_identifier.as_deref(),
                ..Default::default()
            },
        });
        encode_packet(&minimal_connack, &mut connack_buf);
    }
    if connack_buf.len() > client_maximum_packet_size as usize {
        warn!(
            "Client '{}' Maximum Packet Size {} cannot accommodate mandatory CONNACK ({} bytes)",
            client_id,
            client_maximum_packet_size,
            connack_buf.len()
        );
        let _ = socket.shutdown().await;
        return Ok(());
    }

    // A reconnect continuing the same persistent Session cancels a delayed Will.
    // Clean Start ends the previous Session, so a pending Will must be published now.
    if clean_start {
        let _ = state.publish_pending_will_now(&client_id).await;
    } else {
        let _ = state.cancel_pending_will(&client_id).await;
    }

    // Clean Start discards the previous Session before the replacement is installed.
    // Keep the old Arc so an active connection can still receive DISCONNECT 0x8E.
    if clean_start && existing.is_some() {
        state.discard_session_state(&client_id).await;
    }

    // 2. Set up channels for sending outgoing packets to this client
    let (tx, mut rx) = mpsc::channel::<OutboundPacket>(state.client_queue_capacity);

    let auth_username = username.as_deref().unwrap_or("");
    let allow_all_read = state.auth.authorizes_all(auth_username, "read");
    let allow_all_write = state.auth.authorizes_all(auth_username, "write");
    let new_will = will.clone();
    let session = Arc::new(ClientSession::new(
        client_id.clone(),
        username,
        clean_start,
        allow_all_read,
        allow_all_write,
        keep_alive,
        client_receive_maximum,
        client_maximum_packet_size,
        session_expiry_interval,
        tx,
    ));
    *session.will.write() = will;
    if session_present {
        if let Some(old) = existing.as_ref() {
            state.inherit_persistent_state(&session, old);
        }
    }

    // Install the replacement as the current owner before waking an old connection.
    // This closes the takeover race where the old task could otherwise clean up the
    // ClientID while it was still registered as current.
    state.register_session(session.clone()).await;

    if let Some(old) = existing.as_ref() {
        if old.connected.load(Ordering::Acquire) {
            state
                .metrics_session_takeovers
                .fetch_add(1, Ordering::Relaxed);
            let takeover = Packet::Disconnect(crate::codec::Disconnect {
                reason_code: 0x8E, // Session taken over
                properties: Default::default(),
            });
            let mut takeover_buf = Vec::new();
            encode_packet(&takeover, &mut takeover_buf);
            let _ = state.send_to_session(old, takeover_buf).await;

            let old_will = { old.will.write().take() };
            if let Some(old_will) = old_will {
                // MQTT 5 suppresses the exiting connection's delayed Will only when
                // the new connection continues the Session (Clean Start=0) and delay>0.
                if clean_start || old_will.delay_interval == 0 || old.session_expiry() == 0 {
                    state.publish_will_for_client(&client_id, old_will).await;
                } else {
                    // Continuing the same Session before Will Delay expires suppresses
                    // the exiting connection's Will, including its persisted copy.
                    state.clear_persisted_will(&client_id).await;
                }
            } else {
                state.clear_persisted_will(&client_id).await;
            }
            old.request_disconnect();
        }
    }
    drop(existing);

    if let Some(ref message) = new_will {
        state
            .persist_connection_will(
                &client_id,
                session.username.clone(),
                message,
                session.session_expiry(),
            )
            .await;
    } else {
        state.clear_persisted_will(&client_id).await;
    }

    // 3. Spawn a dedicated TCP writer task for this client
    let (mut read_half, mut write_half) = tokio::io::split(socket);
    let client_id_clone = client_id.clone();

    let writer_batch_packets = state.writer_batch_packets;
    let writer_batch_bytes = state.writer_batch_bytes;
    let inbound_receive_pending = session.inbound_receive_pending.clone();
    let writer_task = tokio::spawn(async move {
        let mut batch = Vec::with_capacity(writer_batch_bytes.min(256 * 1024));
        let mut release_ids = Vec::with_capacity(writer_batch_packets.min(256));
        while let Some(first) = rx.recv().await {
            batch.clear();
            release_ids.clear();
            batch.extend_from_slice(&first.bytes);
            if let Some(packet_id) = first.release_inbound_packet_id {
                release_ids.push(packet_id);
            }
            let mut packet_count = 1usize;

            // Under load, collapse queued MQTT packets into one socket/TLS write.
            // Ordering is preserved and the bounded channel remains the source of
            // backpressure; light traffic is still written immediately.
            while packet_count < writer_batch_packets && batch.len() < writer_batch_bytes {
                match rx.try_recv() {
                    Ok(packet) => {
                        batch.extend_from_slice(&packet.bytes);
                        if let Some(packet_id) = packet.release_inbound_packet_id {
                            release_ids.push(packet_id);
                        }
                        packet_count += 1;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            if let Err(e) = write_half.write_all(&batch).await {
                warn!("Failed to write to client {}: {:?}", client_id_clone, e);
                break;
            }
            if let Err(e) = write_half.flush().await {
                warn!("Failed to flush client {}: {:?}", client_id_clone, e);
                break;
            }

            if !release_ids.is_empty() {
                let mut pending = inbound_receive_pending.lock();
                for packet_id in &release_ids {
                    pending.remove(packet_id);
                }
            }
        }
        // Ensure socket write half is closed
        let _ = write_half.shutdown().await;
        debug!("Writer task terminated for client {}", client_id_clone);
    });

    // 4. Send the pre-negotiated CONNACK response.
    if !state.send_to_session(&session, connack_buf).await {
        session.request_disconnect();
    }
    if session_present {
        state.resume_persistent_outgoing(&session).await;
    }

    // 5. Main packet reading and processing loop
    let keep_alive_duration = if keep_alive > 0 {
        // MQTT spec recommends 1.5 times the keep alive time
        Duration::from_millis((keep_alive as u64 * 1500) as u64)
    } else {
        Duration::from_secs(3600 * 24) // Extremely large timeout if keep alive is 0
    };

    let mut disconnect_reason: Option<u8> = None;
    let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
        loop {
            if session.disconnect_requested() {
                warn!("Disconnect requested for client '{}'", client_id);
                break;
            }

            // Slow-consumer policy can interrupt an otherwise idle socket read.
            let read_result = tokio::select! {
                _ = session.disconnect_notify.notified() => {
                    warn!("Slow-consumer disconnect triggered for client '{}'", client_id);
                    break;
                }
                result = tokio::time::timeout(keep_alive_duration, read_half.read_buf(&mut read_buf)) => result,
            };

            match read_result {
                Ok(Ok(0)) => {
                    info!("Client '{}' closed connection", client_id);
                    break;
                }
                Ok(Ok(_)) => {
                    // Update session keep-alive timer
                    session.update_activity();

                    // Parse and process all complete packets currently in the read buffer.
                    // The fixed header reveals total packet size before the payload arrives,
                    // allowing us to enforce Maximum Packet Size without buffering it all.
                    loop {
                        match decode_packet_limited(&read_buf, state.maximum_packet_size) {
                            Ok((packet, bytes_read)) => {
                                if let Packet::Disconnect(pkt) = &packet {
                                    if let Some(expiry) = pkt.properties.session_expiry_interval {
                                        let connect_expiry = session.session_expiry();
                                        if connect_expiry == 0 && expiry > 0 {
                                            let protocol_error = Packet::Disconnect(crate::codec::Disconnect {
                                                reason_code: 0x82,
                                                properties: Default::default(),
                                            });
                                            let mut buf = Vec::new();
                                            encode_packet(&protocol_error, &mut buf);
                                            let _ = state.send_to_session(&session, buf).await;
                                            return Err("DISCONNECT cannot raise a zero Session Expiry Interval".into());
                                        }
                                        session.set_session_expiry(expiry);
                                        if expiry > 0 {
                                            state.db
                                                .save_session(
                                                    session.client_id.clone(),
                                                    session.username.clone(),
                                                    session.clean_start,
                                                    expiry,
                                                )
                                                .await;
                                        }
                                    }
                                    disconnect_reason = Some(pkt.reason_code);
                                    read_buf.advance(bytes_read);
                                    return Ok(());
                                }

                                // Zero-routing QoS 0 fast path: authenticated sessions with a
                                // cached global write ACL need no async dispatch, router lock,
                                // bridge lock, allocation, ACK, or persistence work.
                                if let Packet::Publish(pkt) = &packet {
                                    if pkt.qos == 0
                                        && !pkt.retain
                                        && pkt.properties.subscription_identifiers.is_empty()
                                        && pkt.properties.topic_alias.is_none()
                                        && pkt.properties.response_topic.is_none()
                                        && !pkt.topic.is_empty()
                                        && !topic_contains_wildcard(pkt.topic)
                                        && session.allow_all_write
                                        && !state.router.has_routes()
                                        && !state.bridge_active.load(Ordering::Relaxed)
                                    {
                                        let sequence = session
                                            .published_messages
                                            .fetch_add(1, Ordering::Relaxed);
                                        if sequence
                                            & (state.publish_route_latency_sample_rate as u64 - 1)
                                            == 0
                                        {
                                            let started = Instant::now();
                                            state.publish_route_latency.record(started.elapsed());
                                        }
                                        read_buf.advance(bytes_read);
                                        continue;
                                    }
                                }

                                process_client_packet(&packet, &state, &session).await?;
                                read_buf.advance(bytes_read);
                            }
                            Err(codec::CodecError::Incomplete) => {
                                // Wait for more data
                                break;
                            }
                            Err(e) => {
                                warn!("Codec error processing client '{}': {:?}", client_id, e);
                                let reason = match e {
                                    codec::CodecError::ProtocolError => 0x82,
                                    codec::CodecError::PacketTooLarge => 0x95,
                                    _ => 0x81,
                                };
                                disconnect_client_with_reason(&state, &session, reason).await;
                                return Err(e.into());
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!("TCP read error for client '{}': {:?}", client_id, e);
                    return Err(e.into());
                }
                Err(_) => {
                    warn!("Keep-alive timeout expired for client '{}'", client_id);
                    break;
                }
            }
        }
        Ok(())
    }
    .await;

    // 6. Cleanup session on disconnect or error. Only the currently registered
    // Arc may transition the ClientID offline; a taken-over connection cannot erase its replacement.
    let was_current = state.disconnect_session_if_current(&session).await;
    if was_current {
        let will = { session.will.write().take() };
        if let Some(will) = will {
            if disconnect_reason != Some(0x00) {
                state
                    .schedule_will(
                        client_id.clone(),
                        session.username.clone(),
                        will,
                        session.session_expiry(),
                    )
                    .await;
            } else {
                state.clear_persisted_will(&client_id).await;
            }
        } else if disconnect_reason == Some(0x00) {
            state.clear_persisted_will(&client_id).await;
        }
        if session.session_expiry() > 0 {
            state.schedule_session_expiry(session.clone());
        }
        writer_task.abort();
    } else {
        // A taken-over connection has a queued DISCONNECT 0x8E. Detach its writer
        // so it can flush that packet before the old sender is dropped.
        drop(writer_task);
    }

    result
}

#[inline(always)]
fn publish_topic_error_reason(topic: &str) -> Option<u8> {
    let mut non_ascii = false;
    for &byte in topic.as_bytes() {
        if byte == b'+' || byte == b'#' {
            return Some(0x82); // Protocol Error: wildcard in Topic Name
        }
        if byte <= 0x1f || byte == 0x7f {
            return Some(0x81); // Malformed Packet: forbidden MQTT UTF-8 data
        }
        non_ascii |= byte >= 0x80;
    }
    if !non_ascii {
        return None;
    }
    for ch in topic.chars() {
        let cp = ch as u32;
        if (0x80..=0x9f).contains(&cp)
            || (0xfdd0..=0xfdef).contains(&cp)
            || (cp & 0xffff == 0xfffe)
            || (cp & 0xffff == 0xffff)
        {
            return Some(0x81);
        }
    }
    None
}

#[inline(always)]
fn topic_contains_wildcard(topic: &str) -> bool {
    topic
        .as_bytes()
        .iter()
        .any(|byte| *byte == b'+' || *byte == b'#')
}

async fn disconnect_client_with_reason(
    state: &BrokerState,
    session: &ClientSession,
    reason_code: u8,
) {
    let disconnect = Packet::Disconnect(crate::codec::Disconnect {
        reason_code,
        properties: Default::default(),
    });
    let mut buf = Vec::new();
    encode_packet(&disconnect, &mut buf);
    if state.send_to_session(session, buf).await {
        // Protocol-error paths are cold. Give the dedicated writer a scheduling
        // opportunity to put DISCONNECT on the wire before connection cleanup.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn protocol_error<T>(
    state: &BrokerState,
    session: &ClientSession,
    reason_code: u8,
    message: &'static str,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    disconnect_client_with_reason(state, session, reason_code).await;
    Err(message.into())
}

async fn process_client_packet(
    packet: &Packet<'_>,
    state: &BrokerState,
    session: &ClientSession,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match packet {
        Packet::Publish(pkt) => {
            debug!(
                "Received PUBLISH from client '{}' on topic '{}'",
                session.client_id, pkt.topic
            );

            // Subscription Identifiers in PUBLISH are Server->Client metadata.
            if !pkt.properties.subscription_identifiers.is_empty() {
                return protocol_error(
                    state,
                    session,
                    0x82,
                    "Client PUBLISH contained Subscription Identifier",
                )
                .await;
            }
            if pkt.topic.is_empty() {
                return protocol_error(
                    state,
                    session,
                    0x82,
                    "Zero-length PUBLISH topic without supported Topic Alias",
                )
                .await;
            }
            if let Some(reason) = publish_topic_error_reason(pkt.topic) {
                return protocol_error(
                    state,
                    session,
                    reason,
                    "PUBLISH topic contains MQTT-forbidden characters",
                )
                .await;
            }
            if let Some(response_topic) = pkt.properties.response_topic {
                if response_topic.is_empty() || topic_contains_wildcard(response_topic) {
                    return protocol_error(state, session, 0x82, "Invalid PUBLISH Response Topic")
                        .await;
                }
            }
            if let Some(pfi) = pkt.properties.payload_format_indicator {
                if pfi > 1 {
                    return protocol_error(
                        state,
                        session,
                        0x82,
                        "Invalid Payload Format Indicator",
                    )
                    .await;
                }
            }
            // Pipistrelle currently advertises no Topic Alias Maximum, so a client
            // must not send a Topic Alias. The property is connection-local and is
            // never forwarded to subscribers.
            if pkt.properties.topic_alias.is_some() {
                return protocol_error(
                    state,
                    session,
                    0x94,
                    "Topic Alias received while server maximum is zero",
                )
                .await;
            }

            let inbound_packet_id = if pkt.qos > 0 {
                let Some(packet_id) = pkt.packet_id else {
                    return protocol_error(
                        state,
                        session,
                        0x82,
                        "QoS PUBLISH missing packet identifier",
                    )
                    .await;
                };
                if session
                    .reserve_inbound_receive(packet_id, state.receive_maximum)
                    .is_err()
                {
                    return protocol_error(state, session, 0x93, "Receive Maximum exceeded").await;
                }
                Some(packet_id)
            } else {
                None
            };

            let application_properties = ApplicationProperties::from_publish(&pkt.properties);

            // Check write authorization
            let username = session.username.as_deref().unwrap_or("");
            if !session.allow_all_write && !state.auth.authorize(username, pkt.topic, "write") {
                warn!(
                    "Client '{}' (user: '{}') not authorized to publish on topic '{}'",
                    session.client_id, username, pkt.topic
                );
                if let Some(pid) = pkt.packet_id {
                    let ack = PubAck {
                        packet_id: pid,
                        reason_code: 0x87, // Not Authorized
                        properties: Default::default(),
                    };
                    let packet = if pkt.qos == 2 {
                        Packet::PubRec(ack)
                    } else {
                        Packet::PubAck(ack)
                    };
                    if pkt.qos > 0 {
                        let mut buf = Vec::new();
                        encode_packet(&packet, &mut buf);
                        let _ = state
                            .send_to_session_after_write(session, buf, inbound_packet_id)
                            .await;
                    }
                }
                return Ok(());
            }

            if pkt.qos == 2 {
                let Some(packet_id) = pkt.packet_id else {
                    return protocol_error(
                        state,
                        session,
                        0x82,
                        "QoS 2 PUBLISH missing packet identifier",
                    )
                    .await;
                };
                let already_owned = session.incoming_qos2.read().contains_key(&packet_id);
                if !already_owned {
                    let message = IncomingQos2Message {
                        topic: pkt.topic.to_string(),
                        payload: pkt.payload.to_vec(),
                        retain: pkt.retain,
                        properties: application_properties.clone(),
                    };
                    session
                        .incoming_qos2
                        .write()
                        .insert(packet_id, message.clone());
                    if session.session_expiry() > 0 {
                        state
                            .db
                            .save_qos2_incoming(
                                session.client_id.clone(),
                                packet_id,
                                message.topic,
                                message.payload,
                                message.retain,
                                serde_json::to_string(&message.properties)
                                    .unwrap_or_else(|_| "{}".into()),
                            )
                            .await;
                    }
                }
                // Duplicate QoS 2 PUBLISH with the same packet identifier is acknowledged
                // again but never routed twice while ownership is pending.
                let pubrec = Packet::PubRec(PubAck {
                    packet_id,
                    reason_code: 0x00,
                    properties: Default::default(),
                });
                let mut buf = Vec::new();
                encode_packet(&pubrec, &mut buf);
                let _ = state
                    .send_to_session_after_write(session, buf, Some(packet_id))
                    .await;
                return Ok(());
            }

            if pkt.retain {
                state.update_retained(pkt.topic, pkt.payload, pkt.qos, &application_properties);
            }

            let publish_sequence = session.published_messages.fetch_add(1, Ordering::Relaxed);
            state
                .route_publish(
                    &session.client_id,
                    pkt.topic,
                    pkt.payload,
                    pkt.qos,
                    pkt.retain,
                    &application_properties,
                    publish_sequence,
                )
                .await;

            // If QoS 1, respond with PUBACK
            if pkt.qos == 1 {
                if let Some(pid) = pkt.packet_id {
                    let puback = Packet::PubAck(PubAck {
                        packet_id: pid,
                        reason_code: 0, // Success
                        properties: Default::default(),
                    });
                    let mut buf = Vec::new();
                    encode_packet(&puback, &mut buf);
                    let _ = state
                        .send_to_session_after_write(session, buf, Some(pid))
                        .await;
                }
            }
        }
        Packet::Subscribe(pkt) => {
            debug!("Received SUBSCRIBE from client '{}'", session.client_id);
            let mut reason_codes = Vec::new();
            let mut retained_replays: Vec<(String, u8, Option<u32>)> = Vec::new();
            let username = session.username.as_deref().unwrap_or("");

            for sub in &pkt.subscriptions {
                if !topic_filter_valid(sub.topic_filter) {
                    return protocol_error(state, session, 0x82, "Malformed MQTT Topic Filter")
                        .await;
                }
                let requested_qos = sub.options & 0x03;
                let no_local = sub.options & 0x04 != 0;
                let retain_handling = (sub.options >> 4) & 0x03;
                let malformed =
                    requested_qos == 3 || retain_handling == 3 || sub.options & 0xC0 != 0;
                let shared_no_local = no_local && sub.topic_filter.starts_with("$share/");
                if malformed || shared_no_local {
                    let reason_code = if malformed { 0x81 } else { 0x82 };
                    let disconnect = Packet::Disconnect(crate::codec::Disconnect {
                        reason_code,
                        properties: Default::default(),
                    });
                    let mut buf = Vec::new();
                    encode_packet(&disconnect, &mut buf);
                    let _ = state.send_to_session(session, buf).await;
                    return Err("invalid MQTT v5 subscription options".into());
                }

                if session.allow_all_read
                    || state.auth.authorize(username, sub.topic_filter, "read")
                {
                    let outcome = state.subscribe(
                        &session.client_id,
                        sub.topic_filter,
                        requested_qos,
                        pkt.properties.subscription_identifier,
                        sub.options,
                    );
                    if outcome.accepted {
                        reason_codes.push(requested_qos);
                        if !sub.topic_filter.starts_with("$share/")
                            && (retain_handling == 0 || (retain_handling == 1 && outcome.is_new))
                        {
                            retained_replays.push((
                                sub.topic_filter.to_string(),
                                requested_qos,
                                pkt.properties.subscription_identifier,
                            ));
                        }
                    } else {
                        reason_codes.push(0x97); // Quota exceeded
                    }
                } else {
                    warn!(
                        "Client '{}' (user: '{}') not authorized to subscribe to filter '{}'",
                        session.client_id, username, sub.topic_filter
                    );
                    reason_codes.push(0x87); // Not Authorized
                }
            }

            let suback = Packet::SubAck(SubAck {
                packet_id: pkt.packet_id,
                properties: Default::default(),
                reason_codes,
            });
            let mut buf = Vec::new();
            encode_packet(&suback, &mut buf);
            let _ = state.send_to_session(session, buf).await;

            // Retained PUBLISH packets follow the SUBACK and always carry RETAIN=1.
            for (filter, qos, subscription_identifier) in retained_replays {
                state
                    .send_retained_for_subscription(session, &filter, qos, subscription_identifier)
                    .await;
            }
        }
        Packet::Unsubscribe(pkt) => {
            let mut reason_codes = Vec::with_capacity(pkt.topic_filters.len());
            for filter in &pkt.topic_filters {
                if !topic_filter_valid(filter) {
                    return protocol_error(
                        state,
                        session,
                        0x82,
                        "Malformed MQTT Topic Filter in UNSUBSCRIBE",
                    )
                    .await;
                }
                reason_codes.push(if state.unsubscribe(&session.client_id, filter).await {
                    0x00 // Success
                } else {
                    0x11 // No subscription existed
                });
            }
            let ack = Packet::UnsubAck(UnsubAck {
                packet_id: pkt.packet_id,
                properties: Default::default(),
                reason_codes,
            });
            let mut buf = Vec::new();
            encode_packet(&ack, &mut buf);
            let _ = state.send_to_session(session, buf).await;
        }
        Packet::PubAck(pkt) => {
            debug!(
                "Received PUBACK from client '{}' for packet ID {}",
                session.client_id, pkt.packet_id
            );
            session.release_outbound_slot(pkt.packet_id);
            session.remove_in_flight(pkt.packet_id);

            // Delete from database
            let db = state.db.clone();
            let cid = session.client_id.clone();
            let pid = pkt.packet_id;
            tokio::spawn(async move {
                db.delete_in_flight(cid, pid).await;
            });
            state.drain_receive_maximum(session).await;
        }
        Packet::PubRec(pkt) => {
            let mut resend_pubrel = false;
            let mut discard = false;
            let mut persist_transition = None;
            {
                let mut outgoing = session.outgoing_qos2.write();
                if let Some(message) = outgoing.get_mut(&pkt.packet_id) {
                    if pkt.reason_code >= 0x80 {
                        discard = true;
                    } else {
                        message.phase = OutgoingQos2Phase::AwaitPubComp;
                        message.delivery_started = true;
                        resend_pubrel = true;
                        if session.session_expiry() > 0 {
                            persist_transition = Some(message.clone());
                        }
                    }
                }
                if discard {
                    outgoing.remove(&pkt.packet_id);
                }
            }
            if let Some(persisted) = persist_transition {
                state
                    .db
                    .save_qos2_outgoing(
                        session.client_id.clone(),
                        pkt.packet_id,
                        persisted.topic,
                        persisted.payload,
                        persisted.retain,
                        persisted.subscription_identifier,
                        serde_json::to_string(&persisted.properties)
                            .unwrap_or_else(|_| "{}".into()),
                        persisted.enqueue_order,
                        persisted.delivery_started,
                        persisted.phase as u8,
                    )
                    .await;
            }
            if discard {
                session.release_outbound_slot(pkt.packet_id);
                let db = state.db.clone();
                let cid = session.client_id.clone();
                let pid = pkt.packet_id;
                tokio::spawn(async move { db.delete_qos2_outgoing(cid, pid).await });
                state.drain_receive_maximum(session).await;
            } else if resend_pubrel {
                let pubrel = Packet::PubRel(PubAck {
                    packet_id: pkt.packet_id,
                    reason_code: 0x00,
                    properties: Default::default(),
                });
                let mut buf = Vec::new();
                encode_packet(&pubrel, &mut buf);
                let _ = state.send_to_session(session, buf).await;
            }
        }
        Packet::PubRel(pkt) => {
            let pending = session.incoming_qos2.write().remove(&pkt.packet_id);
            let reason_code = if let Some(message) = pending {
                if message.retain {
                    state.update_retained(&message.topic, &message.payload, 2, &message.properties);
                }
                let sequence = session.published_messages.fetch_add(1, Ordering::Relaxed);
                state
                    .route_publish(
                        &session.client_id,
                        &message.topic,
                        &message.payload,
                        2,
                        message.retain,
                        &message.properties,
                        sequence,
                    )
                    .await;
                let db = state.db.clone();
                let cid = session.client_id.clone();
                let pid = pkt.packet_id;
                tokio::spawn(async move { db.delete_qos2_incoming(cid, pid).await });
                0x00
            } else {
                0x92 // Packet Identifier not found
            };
            let pubcomp = Packet::PubComp(PubAck {
                packet_id: pkt.packet_id,
                reason_code,
                properties: Default::default(),
            });
            let mut buf = Vec::new();
            encode_packet(&pubcomp, &mut buf);
            let _ = state.send_to_session(session, buf).await;
        }
        Packet::PubComp(pkt) => {
            session.release_outbound_slot(pkt.packet_id);
            let removed = session.outgoing_qos2.write().remove(&pkt.packet_id);
            if removed.is_some() {
                let db = state.db.clone();
                let cid = session.client_id.clone();
                let pid = pkt.packet_id;
                tokio::spawn(async move { db.delete_qos2_outgoing(cid, pid).await });
                state.drain_receive_maximum(session).await;
            }
        }
        Packet::PingReq => {
            debug!("Received PINGREQ from client '{}'", session.client_id);
            let pingresp = Packet::PingResp;
            let mut buf = Vec::new();
            encode_packet(&pingresp, &mut buf);
            let _ = state.send_to_session(session, buf).await;
        }
        Packet::Disconnect(_) => {
            info!("Received DISCONNECT from client '{}'", session.client_id);
            // The read loop will naturally exit because we stop processing
        }
        Packet::Connect(_) => {
            warn!(
                "Client '{}' sent CONNECT packet mid-session, violating protocol",
                session.client_id
            );
            return Err("Protocol violation: duplicate CONNECT".into());
        }
        other => {
            warn!(
                "Unsupported packet type from client '{}': {:?}",
                session.client_id, other
            );
        }
    }
    Ok(())
}

// Helper functions for loading certificates and private keys
fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let certfile = File::open(path)?;
    let mut reader = BufReader::new(certfile);
    let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut reader).collect();
    certs.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let keyfile = File::open(path)?;
    let mut reader = BufReader::new(keyfile);
    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "private key not found"))
}
