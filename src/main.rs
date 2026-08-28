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
use std::borrow::Cow;
use std::fs::File;
use std::io::{self, BufReader, IoSlice};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{Level, debug, error, info, warn};
use tracing_subscriber::FmtSubscriber;

use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use smallvec::SmallVec;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

use crate::codec::{
    ConnAck, Packet, PubAck, SubAck, UnsubAck, decode_packet_limited, encode_packet,
};
use crate::router::topic_filter_valid;
use crate::session::{
    ApplicationProperties, BrokerState, ClientSession, IncomingQos2Message, OutboundQueue,
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
                    if let Err(error) = socket.set_nodelay(true) {
                        debug!("Failed to set TCP_NODELAY for {}: {:?}", addr, error);
                    }
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
                        if let Err(error) = socket.set_nodelay(true) {
                            debug!("Failed to set TCP_NODELAY for TLS {}: {:?}", addr, error);
                        }
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
                    if let Err(error) = socket.set_nodelay(true) {
                        debug!(
                            "Failed to set TCP_NODELAY for WebSocket {}: {:?}",
                            addr, error
                        );
                    }
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

    let mut read_buf = BytesMut::with_capacity(CLIENT_READ_BUFFER_INITIAL);

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
        client_outbound_topic_alias_maximum,
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
                let client_outbound_topic_alias_maximum =
                    pkt.properties.topic_alias_maximum.unwrap_or(0);
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
                    client_outbound_topic_alias_maximum,
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
    let mut negotiated_topic_alias_maximum = state.topic_alias_maximum;
    let full_connack = Packet::ConnAck(ConnAck {
        session_present,
        reason_code: 0,
        properties: crate::codec::ConnAckProperties {
            receive_maximum: Some(state.receive_maximum),
            maximum_packet_size: Some(state.maximum_packet_size as u32),
            assigned_client_identifier: assigned_client_identifier.as_deref(),
            topic_alias_maximum: Some(state.topic_alias_maximum),
            ..Default::default()
        },
    });
    encode_packet(&full_connack, &mut connack_buf);
    if connack_buf.len() > client_maximum_packet_size as usize {
        connack_buf.clear();
        negotiated_topic_alias_maximum = 0;
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
    let tx = Arc::new(OutboundQueue::new(state.client_queue_capacity));

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
        negotiated_topic_alias_maximum,
        session_expiry_interval,
        tx,
    ));
    session.set_outbound_topic_alias_maximum(client_outbound_topic_alias_maximum);
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
    let outbound_queue = session.sender.clone();
    let writer_task = tokio::spawn(async move {
        let mut packets = Vec::with_capacity(writer_batch_packets.min(4096));
        let mut batch = Vec::with_capacity(writer_batch_bytes.min(256 * 1024));
        let mut release_ids = Vec::with_capacity(writer_batch_packets.min(256));
        while outbound_queue
            .recv_batch(&mut packets, writer_batch_packets, writer_batch_bytes)
            .await
        {
            batch.clear();
            release_ids.clear();
            for packet in &packets {
                if let Some(packet_id) = packet.release_inbound_packet_id {
                    release_ids.push(packet_id);
                }
            }

            // Aggregated QoS0 descriptors already contain wire-ready contiguous MQTT
            // packets. Avoid copying them into another Vec: direct writes trade a small
            // number of large socket calls for the otherwise dominant memcpy cost.
            let all_plain_blocks = packets
                .iter()
                .all(|packet| packet.release_inbound_packet_id.is_none());
            let write_result = if all_plain_blocks && write_half.is_write_vectored() {
                let mut index = 0usize;
                let mut offset = 0usize;
                let mut result = Ok(());
                while index < packets.len() {
                    let mut slices: SmallVec<[IoSlice<'_>; 64]> = SmallVec::new();
                    slices.push(IoSlice::new(&packets[index].bytes[offset..]));
                    for packet in &packets[index + 1..] {
                        slices.push(IoSlice::new(&packet.bytes));
                    }
                    match write_half.write_vectored(&slices).await {
                        Ok(0) => {
                            result = Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "vectored MQTT writer made no progress",
                            ));
                            break;
                        }
                        Ok(mut written) => {
                            while index < packets.len() {
                                let remaining = packets[index].bytes.len() - offset;
                                if written < remaining {
                                    offset += written;
                                    break;
                                }
                                written -= remaining;
                                index += 1;
                                offset = 0;
                                if written == 0 {
                                    break;
                                }
                            }
                        }
                        Err(error) => {
                            result = Err(error);
                            break;
                        }
                    }
                }
                result
            } else if all_plain_blocks {
                let mut result = Ok(());
                for packet in &packets {
                    if let Err(error) = write_half.write_all(&packet.bytes).await {
                        result = Err(error);
                        break;
                    }
                }
                result
            } else {
                for packet in &packets {
                    batch.extend_from_slice(&packet.bytes);
                }
                write_half.write_all(&batch).await
            };
            if let Err(e) = write_result {
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
        outbound_queue.close();
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
    let mut read_reserve_target = CLIENT_READ_BUFFER_INITIAL;
    let mut saturated_reads = 0u8;
    let mut fast_ingest_layout = FastIngestLayout::default();
    let mut fast_ingest_alias_layout = FastIngestLayout::default();
    let mut fast_route_layout = FastIngestLayout::default();
    let mut fast_route_alias_layout = FastIngestLayout::default();
    let mut fast_exact_route = FastExactRouteCache::default();
    let mut fast_alias_route = FastAliasRouteCache::default();
    let mut fast_alias_egress = FastAliasEgressCache::default();
    let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
        loop {
            if session.disconnect_requested() {
                warn!("Disconnect requested for client '{}'", client_id);
                break;
            }

            // Hot publishers grow their receive buffer adaptively. Idle/normal clients
            // remain at 4 KiB, while sustained full reads can reach 64 KiB and amortize
            // Tokio/select/syscall overhead across hundreds of MQTT packets.
            if read_buf.capacity().saturating_sub(read_buf.len()) < read_reserve_target {
                read_buf.reserve(read_reserve_target);
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
                Ok(Ok(bytes_read_from_socket)) => {
                    if bytes_read_from_socket >= read_reserve_target.saturating_sub(512) {
                        saturated_reads = saturated_reads.saturating_add(1);
                        let read_buffer_max = if session.allow_all_write
                            && !state.bridge_active.load(Ordering::Relaxed)
                            && fast_ingest_alias_layout.total_len != 0
                            && !state.router.has_routes()
                        {
                            CLIENT_READ_BUFFER_MAX_FAST_ALIAS_INGEST
                        } else if session.allow_all_write
                            && !state.bridge_active.load(Ordering::Relaxed)
                            && fast_route_alias_layout.total_len != 0
                            && state.router.has_only_exact_routes()
                        {
                            CLIENT_READ_BUFFER_MAX_FAST_ALIAS_ROUTE
                        } else {
                            CLIENT_READ_BUFFER_MAX_HOT
                        };
                        if saturated_reads >= 2 && read_reserve_target < read_buffer_max {
                            read_reserve_target = (read_reserve_target * 2).min(read_buffer_max);
                            saturated_reads = 0;
                        }
                    } else {
                        saturated_reads = 0;
                    }

                    // Parse and process all complete packets currently in the read buffer.
                    // The dominant zero-route QoS0 case is scanned as a native batch: no
                    // Packet enum, property object, async dispatch, or per-message atomic.
                    loop {
                        if session.allow_all_write
                            && !state.bridge_active.load(Ordering::Relaxed)
                        {
                            let route_epoch = state.router.mutation_epoch();
                            if !state.router.has_routes() {
                                let mut batch = scan_zero_route_qos0_batch(
                                    &read_buf,
                                    state.maximum_packet_size,
                                    &mut fast_ingest_layout,
                                );
                                if batch.messages == 0 && session.topic_alias_maximum > 0 {
                                    batch = scan_zero_route_qos0_alias_batch(
                                        &read_buf,
                                        state.maximum_packet_size,
                                        &mut fast_ingest_alias_layout,
                                        &session,
                                    );
                                }
                                // A concurrent SUBSCRIBE/UNSUBSCRIBE/upsert invalidates the
                                // zero-route assumption. Do not advance the buffer in that case;
                                // the general decoder below will route every packet normally.
                                if batch.messages != 0
                                    && route_epoch == state.router.mutation_epoch()
                                    && !state.router.has_routes()
                                    && !state.bridge_active.load(Ordering::Relaxed)
                                {
                                    let start = session
                                        .published_messages
                                        .fetch_add(batch.messages, Ordering::Relaxed);
                                    let samples = sampled_sequences_in_range(
                                        start,
                                        batch.messages,
                                        state.publish_route_latency_sample_rate as u64,
                                    );
                                    state
                                        .publish_route_latency
                                        .record_zero_route_samples(samples);
                                    read_buf.advance(batch.bytes);
                                    continue;
                                }
                            } else if state.router.has_only_exact_routes() {
                                let batch = scan_zero_route_qos0_batch(
                                    &read_buf,
                                    state.maximum_packet_size,
                                    &mut fast_route_layout,
                                );
                                if batch.messages != 0 {
                                    if let Some(target) = resolve_fast_exact_route(
                                        &state,
                                        &session,
                                        &fast_route_layout,
                                        &mut fast_exact_route,
                                    ) {
                                        let session_epoch =
                                            state.session_epoch.load(Ordering::Acquire);
                                        if route_epoch == state.router.mutation_epoch()
                                            && session_epoch == fast_exact_route.session_epoch
                                            && !state.bridge_active.load(Ordering::Relaxed)
                                        {
                                            let total = fast_route_layout.total_len;
                                            debug_assert_eq!(
                                                batch.bytes,
                                                total * batch.messages as usize
                                            );
                                            let routed = read_buf.split_to(batch.bytes).freeze();
                                            let start = session
                                                .published_messages
                                                .fetch_add(batch.messages, Ordering::Relaxed);
                                            let samples = sampled_sequences_in_range(
                                                start,
                                                batch.messages,
                                                state.publish_route_latency_sample_rate as u64,
                                            );
                                            let route_started =
                                                (samples != 0).then(fast_route_timestamp);
                                            let _ = state
                                                .send_bytes_batch_to_session(
                                                    &target,
                                                    routed,
                                                    total,
                                                    batch.messages as usize,
                                                )
                                                .await;
                                            if let Some(started) = route_started {
                                                state.publish_route_latency.record_repeated(
                                                    fast_route_elapsed(started),
                                                    samples,
                                                );
                                            }
                                            continue;
                                        }
                                    }
                                }

                                if session.topic_alias_maximum > 0 {
                                    let alias_batch = scan_zero_route_qos0_alias_batch(
                                        &read_buf,
                                        state.maximum_packet_size,
                                        &mut fast_route_alias_layout,
                                        &session,
                                    );
                                    if alias_batch.messages != 0 {
                                        if let Some((target, topic, outbound_alias)) =
                                            resolve_fast_alias_exact_route(
                                                &state,
                                                &session,
                                                &fast_route_alias_layout,
                                                &mut fast_exact_route,
                                                &mut fast_alias_route,
                                            )
                                        {
                                            let session_epoch =
                                                state.session_epoch.load(Ordering::Acquire);
                                            if route_epoch == state.router.mutation_epoch()
                                                && session_epoch == fast_exact_route.session_epoch
                                                && !state.bridge_active.load(Ordering::Relaxed)
                                            {
                                                let input_alias = fast_route_alias_layout.alias;
                                                if let (Some(input_alias), Some((alias, established))) =
                                                    (input_alias, outbound_alias)
                                                {
                                                    if input_alias == alias {
                                                        let total = fast_route_alias_layout.total_len;
                                                        let payload_offset =
                                                            fast_route_alias_layout.prefix.len();
                                                        let count = alias_batch.messages as usize;
                                                        if total != 0
                                                            && payload_offset <= total
                                                            && alias_batch.bytes
                                                                == total.saturating_mul(count)
                                                        {
                                                            let mapping_ready = if established {
                                                                true
                                                            } else {
                                                                let payload = &read_buf
                                                                    [payload_offset..total];
                                                                let mapping = crate::codec::
                                                                    encode_publish_qos0_with_topic_alias(
                                                                        &topic, payload, alias,
                                                                    );
                                                                let sent = state
                                                                    .send_to_session(
                                                                        &target, mapping,
                                                                    )
                                                                    .await;
                                                                if sent {
                                                                    target
                                                                        .mark_outbound_topic_alias_established(
                                                                            &topic, alias,
                                                                        );
                                                                    if fast_alias_route.outbound_alias == Some(alias) {
                                                                        fast_alias_route.outbound_established = true;
                                                                    }
                                                                }
                                                                sent
                                                            };
                                                            if mapping_ready {
                                                                let routed = read_buf
                                                                    .split_to(alias_batch.bytes)
                                                                    .freeze();
                                                                let direct_count = if established {
                                                                    count
                                                                } else {
                                                                    count.saturating_sub(1)
                                                                };
                                                                let direct = if established {
                                                                    routed
                                                                } else {
                                                                    routed.slice(total..)
                                                                };
                                                                let start = session
                                                                    .published_messages
                                                                    .fetch_add(
                                                                        alias_batch.messages,
                                                                        Ordering::Relaxed,
                                                                    );
                                                                let samples =
                                                                    sampled_sequences_in_range(
                                                                        start,
                                                                        alias_batch.messages,
                                                                        state
                                                                            .publish_route_latency_sample_rate
                                                                            as u64,
                                                                    );
                                                                let route_started = (samples != 0)
                                                                    .then(fast_route_timestamp);
                                                                if direct_count != 0 {
                                                                    let _ = state
                                                                        .send_bytes_batch_to_session(
                                                                            &target,
                                                                            direct,
                                                                            total,
                                                                            direct_count,
                                                                        )
                                                                        .await;
                                                                }
                                                                if let Some(started) = route_started {
                                                                    state.publish_route_latency.record_repeated(
                                                                        fast_route_elapsed(started),
                                                                        samples,
                                                                    );
                                                                }
                                                                continue;
                                                            }
                                                        }
                                                    } else {
                                                        let total = fast_route_alias_layout.total_len;
                                                        let payload_offset =
                                                            fast_route_alias_layout.prefix.len();
                                                        let count = alias_batch.messages as usize;
                                                        if total != 0
                                                            && payload_offset <= total
                                                            && alias_batch.bytes
                                                                == total.saturating_mul(count)
                                                        {
                                                            let mapping_ready = if established {
                                                                true
                                                            } else {
                                                                let payload = &read_buf
                                                                    [payload_offset..total];
                                                                let mapping = crate::codec::
                                                                    encode_publish_qos0_with_topic_alias(
                                                                        &topic, payload, alias,
                                                                    );
                                                                let sent = state
                                                                    .send_to_session(&target, mapping)
                                                                    .await;
                                                                if sent {
                                                                    target
                                                                        .mark_outbound_topic_alias_established(
                                                                            &topic, alias,
                                                                        );
                                                                    if fast_alias_route.outbound_alias == Some(alias) {
                                                                        fast_alias_route.outbound_established = true;
                                                                    }
                                                                }
                                                                sent
                                                            };
                                                            if mapping_ready {
                                                                let source_start = if established { 0 } else { total };
                                                                let direct_count = if established {
                                                                    count
                                                                } else {
                                                                    count.saturating_sub(1)
                                                                };
                                                                let rewritten = if direct_count == 0 {
                                                                    Some(bytes::Bytes::new())
                                                                } else {
                                                                    rewrite_fast_alias_batch(
                                                                        &read_buf[source_start..alias_batch.bytes],
                                                                        FastIngestBatch {
                                                                            bytes: alias_batch.bytes - source_start,
                                                                            messages: direct_count as u64,
                                                                        },
                                                                        &fast_route_alias_layout,
                                                                        alias,
                                                                    )
                                                                };
                                                                if let Some(rewritten) = rewritten {
                                                                    read_buf.advance(alias_batch.bytes);
                                                                    let start = session
                                                                        .published_messages
                                                                        .fetch_add(
                                                                            alias_batch.messages,
                                                                            Ordering::Relaxed,
                                                                        );
                                                                    let samples =
                                                                        sampled_sequences_in_range(
                                                                            start,
                                                                            alias_batch.messages,
                                                                            state
                                                                                .publish_route_latency_sample_rate
                                                                                as u64,
                                                                        );
                                                                    let route_started = (samples != 0)
                                                                        .then(fast_route_timestamp);
                                                                    if direct_count != 0 {
                                                                        let _ = state
                                                                            .send_bytes_batch_to_session(
                                                                                &target,
                                                                                rewritten,
                                                                                total,
                                                                                direct_count,
                                                                            )
                                                                            .await;
                                                                    }
                                                                    if let Some(started) = route_started {
                                                                        state.publish_route_latency.record_repeated(
                                                                            fast_route_elapsed(started),
                                                                            samples,
                                                                        );
                                                                    }
                                                                    continue;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }

                                                if let Some((routed, packet_len)) =
                                                    expand_fast_alias_batch(
                                                        &read_buf[..alias_batch.bytes],
                                                        alias_batch,
                                                        &fast_route_alias_layout,
                                                        &topic,
                                                        &mut fast_alias_egress,
                                                    )
                                                {
                                                    read_buf.advance(alias_batch.bytes);
                                                    let start = session.published_messages.fetch_add(
                                                        alias_batch.messages,
                                                        Ordering::Relaxed,
                                                    );
                                                    let samples = sampled_sequences_in_range(
                                                        start,
                                                        alias_batch.messages,
                                                        state.publish_route_latency_sample_rate
                                                            as u64,
                                                    );
                                                    let route_started = (samples != 0)
                                                        .then(fast_route_timestamp);
                                                    let _ = state
                                                        .send_bytes_batch_to_session(
                                                            &target,
                                                            routed,
                                                            packet_len,
                                                            alias_batch.messages as usize,
                                                        )
                                                        .await;
                                                    if let Some(started) = route_started {
                                                        state.publish_route_latency.record_repeated(
                                                            fast_route_elapsed(started),
                                                            samples,
                                                        );
                                                    }
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        match decode_packet_limited(&read_buf, state.maximum_packet_size) {
                            Ok((packet, bytes_read)) => {
                                if let Packet::Disconnect(pkt) = &packet {
                                    if let Some(expiry) = pkt.properties.session_expiry_interval {
                                        let connect_expiry = session.session_expiry();
                                        if connect_expiry == 0 && expiry > 0 {
                                            let protocol_error = Packet::Disconnect(
                                                crate::codec::Disconnect {
                                                    reason_code: 0x82,
                                                    properties: Default::default(),
                                                },
                                            );
                                            let mut buf = Vec::new();
                                            encode_packet(&protocol_error, &mut buf);
                                            let _ = state.send_to_session(&session, buf).await;
                                            return Err(
                                                "DISCONNECT cannot raise a zero Session Expiry Interval"
                                                    .into(),
                                            );
                                        }
                                        session.set_session_expiry(expiry);
                                        if expiry > 0 {
                                            state
                                                .db
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

                                // Zero-routing QoS 0 fallback for legal forms outside the
                                // native layout cache (for example non-ASCII Topic Names).
                                if let Packet::Publish(pkt) = &packet {
                                    if pkt.qos == 0
                                        && !pkt.retain
                                        && pkt.properties.subscription_identifiers.is_empty()
                                        && pkt.properties.topic_alias.is_none()
                                        && pkt.properties.response_topic.is_none()
                                        && !pkt.topic.is_empty()
                                        && publish_topic_error_reason(pkt.topic).is_none()
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

const CLIENT_READ_BUFFER_INITIAL: usize = 4 * 1024;
const CLIENT_READ_BUFFER_MAX_HOT: usize = 128 * 1024;
const CLIENT_READ_BUFFER_MAX_FAST_ALIAS_INGEST: usize = 512 * 1024;
const CLIENT_READ_BUFFER_MAX_FAST_ALIAS_ROUTE: usize = 128 * 1024;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct FastIngestBatch {
    bytes: usize,
    messages: u64,
}

#[derive(Debug, Default)]
struct FastIngestLayout {
    total_len: usize,
    topic: Option<Arc<str>>,
    alias: Option<u16>,
    prefix: Vec<u8>,
    prefix32: [u8; 32],
    mask32: [u8; 32],
    scalar_words: [u64; 3],
    scalar_tail_mask: u64,
    scalar9_word: u64,
    scalar9_tail: u8,
    scalar9: bool,
    scalar24: bool,
    neon32: bool,
}

impl FastIngestLayout {
    #[inline]
    fn cache_prefix(&mut self, prefix: &[u8], total_len: usize) {
        self.total_len = total_len;
        self.alias = None;
        self.prefix.clear();
        self.prefix.extend_from_slice(prefix);
        self.prefix32 = [0; 32];
        self.mask32 = [0; 32];
        self.scalar_words = [0; 3];
        self.scalar_tail_mask = 0;
        self.scalar9_word = 0;
        self.scalar9_tail = 0;
        self.scalar9 = prefix.len() == 9 && total_len >= 9;
        if self.scalar9 {
            self.scalar9_word = u64::from_ne_bytes(prefix[..8].try_into().unwrap());
            self.scalar9_tail = prefix[8];
        }
        self.scalar24 = prefix.len() > 16 && prefix.len() <= 24 && total_len >= 24;
        if self.scalar24 {
            let mut tmp = [0u8; 24];
            tmp[..prefix.len()].copy_from_slice(prefix);
            self.scalar_words[0] = u64::from_ne_bytes(tmp[0..8].try_into().unwrap());
            self.scalar_words[1] = u64::from_ne_bytes(tmp[8..16].try_into().unwrap());
            self.scalar_words[2] = u64::from_ne_bytes(tmp[16..24].try_into().unwrap());
            let tail = prefix.len() - 16;
            self.scalar_tail_mask = if tail == 8 {
                u64::MAX
            } else {
                (1u64 << (tail * 8)) - 1
            };
        }
        self.neon32 = prefix.len() <= 32 && total_len >= 32;
        if self.neon32 {
            self.prefix32[..prefix.len()].copy_from_slice(prefix);
            self.mask32[..prefix.len()].fill(0xff);
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn fast_ingest_layout_matches(buf: &[u8], offset: usize, layout: &FastIngestLayout) -> bool {
    if layout.neon32 {
        use std::arch::aarch64::{vandq_u8, veorq_u8, vld1q_u8, vmaxvq_u8, vorrq_u8};
        debug_assert!(offset + 32 <= buf.len());
        unsafe {
            let actual0 = vld1q_u8(buf.as_ptr().add(offset));
            let actual1 = vld1q_u8(buf.as_ptr().add(offset + 16));
            let expected0 = vld1q_u8(layout.prefix32.as_ptr());
            let expected1 = vld1q_u8(layout.prefix32.as_ptr().add(16));
            let mask0 = vld1q_u8(layout.mask32.as_ptr());
            let mask1 = vld1q_u8(layout.mask32.as_ptr().add(16));
            let diff0 = vandq_u8(veorq_u8(actual0, expected0), mask0);
            let diff1 = vandq_u8(veorq_u8(actual1, expected1), mask1);
            vmaxvq_u8(vorrq_u8(diff0, diff1)) == 0
        }
    } else {
        let prefix_len = layout.prefix.len();
        offset + prefix_len <= buf.len() && buf[offset..offset + prefix_len] == layout.prefix
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn fast_ingest_layout_matches_8(
    buf: &[u8],
    offset: usize,
    total: usize,
    layout: &FastIngestLayout,
) -> bool {
    if layout.scalar24 {
        debug_assert!(offset + total * 8 <= buf.len());
        unsafe {
            let mut any = 0u64;
            for lane in 0..8 {
                let base = buf.as_ptr().add(offset + lane * total);
                let a0 = std::ptr::read_unaligned(base.cast::<u64>());
                let a1 = std::ptr::read_unaligned(base.add(8).cast::<u64>());
                let a2 = std::ptr::read_unaligned(base.add(16).cast::<u64>());
                any |= a0 ^ layout.scalar_words[0];
                any |= a1 ^ layout.scalar_words[1];
                any |= (a2 ^ layout.scalar_words[2]) & layout.scalar_tail_mask;
            }
            return any == 0;
        }
    }
    if !layout.neon32 {
        return (0..8).all(|lane| fast_ingest_layout_matches(buf, offset + lane * total, layout));
    }
    use std::arch::aarch64::{vandq_u8, vdupq_n_u8, veorq_u8, vld1q_u8, vmaxvq_u8, vorrq_u8};
    debug_assert!(offset + total * 8 <= buf.len());
    unsafe {
        let expected0 = vld1q_u8(layout.prefix32.as_ptr());
        let expected1 = vld1q_u8(layout.prefix32.as_ptr().add(16));
        let mask0 = vld1q_u8(layout.mask32.as_ptr());
        let mask1 = vld1q_u8(layout.mask32.as_ptr().add(16));
        let mut any_diff = vdupq_n_u8(0);
        for lane in 0..8 {
            let base = buf.as_ptr().add(offset + lane * total);
            let actual0 = vld1q_u8(base);
            let actual1 = vld1q_u8(base.add(16));
            let diff0 = vandq_u8(veorq_u8(actual0, expected0), mask0);
            let diff1 = vandq_u8(veorq_u8(actual1, expected1), mask1);
            any_diff = vorrq_u8(any_diff, vorrq_u8(diff0, diff1));
        }
        vmaxvq_u8(any_diff) == 0
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn fast_ingest_layout_matches_16_scalar24(
    buf: &[u8],
    offset: usize,
    total: usize,
    layout: &FastIngestLayout,
) -> bool {
    if !layout.scalar24 {
        return false;
    }
    debug_assert!(offset + total * 16 <= buf.len());
    unsafe {
        let mut any = 0u64;
        for lane in 0..16 {
            let base = buf.as_ptr().add(offset + lane * total);
            let a0 = std::ptr::read_unaligned(base.cast::<u64>());
            let a1 = std::ptr::read_unaligned(base.add(8).cast::<u64>());
            let a2 = std::ptr::read_unaligned(base.add(16).cast::<u64>());
            any |= a0 ^ layout.scalar_words[0];
            any |= a1 ^ layout.scalar_words[1];
            any |= (a2 ^ layout.scalar_words[2]) & layout.scalar_tail_mask;
        }
        any == 0
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn fast_ingest_layout_matches_16_scalar9(
    buf: &[u8],
    offset: usize,
    total: usize,
    layout: &FastIngestLayout,
) -> bool {
    if !layout.scalar9 {
        return false;
    }
    debug_assert!(offset + total * 16 <= buf.len());
    unsafe {
        let mut any = 0u64;
        let mut tail = 0u8;
        for lane in 0..16 {
            let base = buf.as_ptr().add(offset + lane * total);
            any |= std::ptr::read_unaligned(base.cast::<u64>()) ^ layout.scalar9_word;
            tail |= *base.add(8) ^ layout.scalar9_tail;
        }
        (any | tail as u64) == 0
    }
}

#[cfg(not(target_arch = "aarch64"))]
#[inline(always)]
fn fast_ingest_layout_matches(buf: &[u8], offset: usize, layout: &FastIngestLayout) -> bool {
    let prefix_len = layout.prefix.len();
    offset + prefix_len <= buf.len() && buf[offset..offset + prefix_len] == layout.prefix
}

#[cfg(not(target_arch = "aarch64"))]
#[inline(always)]
fn fast_ingest_layout_matches_8(
    buf: &[u8],
    offset: usize,
    total: usize,
    layout: &FastIngestLayout,
) -> bool {
    (0..8).all(|lane| fast_ingest_layout_matches(buf, offset + lane * total, layout))
}

#[cfg(not(target_arch = "aarch64"))]
#[inline(always)]
fn fast_ingest_layout_matches_16_scalar24(
    _buf: &[u8],
    _offset: usize,
    _total: usize,
    _layout: &FastIngestLayout,
) -> bool {
    false
}

#[cfg(not(target_arch = "aarch64"))]
#[inline(always)]
fn fast_ingest_layout_matches_16_scalar9(
    _buf: &[u8],
    _offset: usize,
    _total: usize,
    _layout: &FastIngestLayout,
) -> bool {
    false
}

#[derive(Default)]
struct FastExactRouteCache {
    route_epoch: u64,
    session_epoch: u64,
    topic: Option<Arc<str>>,
    target: Option<Arc<ClientSession>>,
}

impl FastExactRouteCache {
    #[inline]
    fn invalidate(&mut self) {
        self.target = None;
        self.topic = None;
    }
}

fn resolve_fast_exact_route_topic(
    state: &BrokerState,
    publisher: &ClientSession,
    topic: &str,
    cache: &mut FastExactRouteCache,
) -> Option<Arc<ClientSession>> {
    let route_epoch = state.router.mutation_epoch();
    let session_epoch = state.session_epoch.load(Ordering::Acquire);

    if cache.route_epoch == route_epoch
        && cache.session_epoch == session_epoch
        && cache.topic.as_deref() == Some(topic)
    {
        if let Some(target) = cache.target.as_ref() {
            if target.connected.load(Ordering::Acquire) {
                return Some(target.clone());
            }
        }
    }

    cache.invalidate();
    cache.route_epoch = route_epoch;
    cache.session_epoch = session_epoch;
    cache.topic = Some(Arc::<str>::from(topic));

    if !state.router.has_only_exact_routes() {
        return None;
    }
    let subscriptions = state.router.match_exact(topic)?;
    if subscriptions.len() != 1 {
        return None;
    }
    let sub = &subscriptions[0];
    if sub.subscription_identifier.is_some()
        || (sub.no_local && sub.client_id == publisher.client_id)
    {
        return None;
    }
    let target = state.sessions.read().get(&sub.client_id).cloned()?;
    if !target.connected.load(Ordering::Acquire) {
        return None;
    }
    cache.target = Some(target.clone());
    Some(target)
}

fn resolve_fast_exact_route(
    state: &BrokerState,
    publisher: &ClientSession,
    layout: &FastIngestLayout,
    cache: &mut FastExactRouteCache,
) -> Option<Arc<ClientSession>> {
    resolve_fast_exact_route_topic(state, publisher, layout.topic.as_deref()?, cache)
}

#[derive(Default)]
struct FastAliasRouteCache {
    route_epoch: u64,
    session_epoch: u64,
    alias_epoch: u64,
    alias: u16,
    topic: Option<Arc<str>>,
    target: Option<Arc<ClientSession>>,
    outbound_alias: Option<u16>,
    outbound_established: bool,
}

impl FastAliasRouteCache {
    fn invalidate(&mut self) {
        self.topic = None;
        self.target = None;
        self.outbound_alias = None;
        self.outbound_established = false;
    }
}

fn resolve_fast_alias_exact_route(
    state: &BrokerState,
    publisher: &ClientSession,
    layout: &FastIngestLayout,
    exact_cache: &mut FastExactRouteCache,
    alias_cache: &mut FastAliasRouteCache,
) -> Option<(Arc<ClientSession>, Arc<str>, Option<(u16, bool)>)> {
    let alias = layout.alias?;
    let route_epoch = state.router.mutation_epoch();
    let session_epoch = state.session_epoch.load(Ordering::Acquire);
    let alias_epoch = publisher.inbound_topic_alias_epoch();

    if alias_cache.alias == alias
        && alias_cache.route_epoch == route_epoch
        && alias_cache.session_epoch == session_epoch
        && alias_cache.alias_epoch == alias_epoch
    {
        if let (Some(topic), Some(target)) =
            (alias_cache.topic.as_ref(), alias_cache.target.as_ref())
        {
            if target.connected.load(Ordering::Acquire) {
                return Some((
                    target.clone(),
                    topic.clone(),
                    alias_cache
                        .outbound_alias
                        .map(|outbound| (outbound, alias_cache.outbound_established)),
                ));
            }
        }
    }

    alias_cache.invalidate();
    alias_cache.alias = alias;
    alias_cache.route_epoch = route_epoch;
    alias_cache.session_epoch = session_epoch;
    alias_cache.alias_epoch = alias_epoch;

    let topic = {
        let aliases = publisher.topic_aliases.read();
        Arc::<str>::from(aliases.get(&alias)?.as_str())
    };
    let target = resolve_fast_exact_route_topic(state, publisher, &topic, exact_cache)?;
    let outbound = target.outbound_topic_alias_for(&topic);
    alias_cache.topic = Some(topic.clone());
    alias_cache.target = Some(target.clone());
    if let Some((outbound_alias, established)) = outbound {
        alias_cache.outbound_alias = Some(outbound_alias);
        alias_cache.outbound_established = established;
    }
    Some((target, topic, outbound))
}

#[derive(Debug, Default)]
struct FastAliasEgressCache {
    topic: Option<Arc<str>>,
    input_total_len: usize,
    payload_offset: usize,
    packet_len: usize,
    prefix: Vec<u8>,
    prefix32: [u8; 32],
    fast_prefix32: bool,
}

impl FastAliasEgressCache {
    fn prepare(&mut self, topic: &Arc<str>, layout: &FastIngestLayout) -> Option<()> {
        let payload_offset = layout.prefix.len();
        if payload_offset > layout.total_len || topic.len() > u16::MAX as usize {
            return None;
        }
        if self.topic.as_deref() == Some(topic.as_ref())
            && self.input_total_len == layout.total_len
            && self.payload_offset == payload_offset
            && self.packet_len != 0
        {
            return Some(());
        }

        let payload_len = layout.total_len.checked_sub(payload_offset)?;
        let remaining_len = 2usize
            .checked_add(topic.len())?
            .checked_add(1)?
            .checked_add(payload_len)?;
        if remaining_len > 268_435_455 {
            return None;
        }

        self.prefix.clear();
        self.prefix.reserve(1 + 4 + 2 + topic.len() + 1);
        self.prefix.push(0x30);
        crate::codec::encode_varint(remaining_len as u32, &mut self.prefix);
        self.prefix
            .extend_from_slice(&(topic.len() as u16).to_be_bytes());
        self.prefix.extend_from_slice(topic.as_bytes());
        self.prefix.push(0); // Topic Alias is connection-local and is not forwarded.
        self.packet_len = self.prefix.len().checked_add(payload_len)?;
        self.prefix32 = [0; 32];
        self.fast_prefix32 = self.prefix.len() <= 32 && self.packet_len >= 32;
        if self.fast_prefix32 {
            self.prefix32[..self.prefix.len()].copy_from_slice(&self.prefix);
        }
        self.input_total_len = layout.total_len;
        self.payload_offset = payload_offset;
        self.topic = Some(topic.clone());
        Some(())
    }
}

fn expand_fast_alias_batch(
    buf: &[u8],
    batch: FastIngestBatch,
    layout: &FastIngestLayout,
    topic: &Arc<str>,
    cache: &mut FastAliasEgressCache,
) -> Option<(bytes::Bytes, usize)> {
    cache.prepare(topic, layout)?;
    let count = usize::try_from(batch.messages).ok()?;
    if count == 0 || batch.bytes != layout.total_len.checked_mul(count)? || batch.bytes > buf.len()
    {
        return None;
    }
    let payload_len = layout.total_len.checked_sub(cache.payload_offset)?;
    let output_len = cache.packet_len.checked_mul(count)?;
    let mut output = Vec::<u8>::with_capacity(output_len);
    unsafe { output.set_len(output_len) };

    #[cfg(target_arch = "aarch64")]
    if cache.fast_prefix32 && payload_len == 128 {
        use std::arch::aarch64::{vld1q_u8, vst1q_u8};
        unsafe {
            let prefix0 = vld1q_u8(cache.prefix32.as_ptr());
            let prefix1 = vld1q_u8(cache.prefix32.as_ptr().add(16));
            let source = buf.as_ptr();
            let destination = output.as_mut_ptr();
            for index in 0..count {
                let source_payload = source.add(index * layout.total_len + cache.payload_offset);
                let dest_packet = destination.add(index * cache.packet_len);
                vst1q_u8(dest_packet, prefix0);
                vst1q_u8(dest_packet.add(16), prefix1);
                for lane in 0..8usize {
                    let value = vld1q_u8(source_payload.add(lane * 16));
                    vst1q_u8(dest_packet.add(cache.prefix.len() + lane * 16), value);
                }
            }
        }
        return Some((bytes::Bytes::from(output), cache.packet_len));
    }

    unsafe {
        let source = buf.as_ptr();
        let destination = output.as_mut_ptr();
        for index in 0..count {
            let input_start = index * layout.total_len;
            let dest_packet = destination.add(index * cache.packet_len);
            std::ptr::copy_nonoverlapping(cache.prefix.as_ptr(), dest_packet, cache.prefix.len());
            std::ptr::copy_nonoverlapping(
                source.add(input_start + cache.payload_offset),
                dest_packet.add(cache.prefix.len()),
                payload_len,
            );
        }
    }
    Some((bytes::Bytes::from(output), cache.packet_len))
}

fn rewrite_fast_alias_batch(
    buf: &[u8],
    batch: FastIngestBatch,
    layout: &FastIngestLayout,
    outbound_alias: u16,
) -> Option<bytes::Bytes> {
    let count = usize::try_from(batch.messages).ok()?;
    if count == 0
        || layout.total_len == 0
        || layout.prefix.len() < 2
        || batch.bytes != layout.total_len.checked_mul(count)?
        || batch.bytes > buf.len()
    {
        return None;
    }
    let alias_offset = layout.prefix.len().checked_sub(2)?;
    if alias_offset + 2 > layout.total_len {
        return None;
    }
    let mut output = buf[..batch.bytes].to_vec();
    let alias = outbound_alias.to_be_bytes();
    for index in 0..count {
        let offset = index
            .checked_mul(layout.total_len)?
            .checked_add(alias_offset)?;
        output[offset] = alias[0];
        output[offset + 1] = alias[1];
    }
    Some(bytes::Bytes::from(output))
}

fn scan_zero_route_qos0_batch(
    buf: &[u8],
    maximum_packet_size: usize,
    layout: &mut FastIngestLayout,
) -> FastIngestBatch {
    let mut offset = 0usize;
    let mut messages = 0u64;

    while offset < buf.len() {
        if layout.total_len != 0 {
            let total = layout.total_len;
            if layout.scalar24 {
                let sixteen = total.saturating_mul(16);
                while offset + sixteen <= buf.len() {
                    if !fast_ingest_layout_matches_16_scalar24(buf, offset, total, layout) {
                        break;
                    }
                    messages += 16;
                    offset += sixteen;
                }
            }
            let eight = total.saturating_mul(8);
            while offset + eight <= buf.len() {
                if !fast_ingest_layout_matches_8(buf, offset, total, layout) {
                    break;
                }
                messages += 8;
                offset += eight;
            }
            if offset + total > buf.len() {
                break;
            }
            if fast_ingest_layout_matches(buf, offset, layout) {
                messages += 1;
                offset += total;
                continue;
            }
        }

        // Fixed header 0x30 means PUBLISH, DUP=0, QoS0, RETAIN=0. Other legal
        // PUBLISH forms and all other control packets use the general codec.
        if buf[offset] != 0x30 {
            break;
        }
        if offset + 1 >= buf.len() {
            break;
        }
        let first_remaining = buf[offset + 1];
        let (remaining_len, remaining_bytes) = if first_remaining & 0x80 == 0 {
            (first_remaining as usize, 1usize)
        } else {
            if offset + 2 >= buf.len() {
                break;
            }
            let second_remaining = buf[offset + 2];
            // The native ingest scanner intentionally handles the common canonical
            // 1/2-byte Remaining Length forms. Larger or non-canonical encodings fall
            // through to the fully general MQTT codec.
            if second_remaining & 0x80 != 0 {
                break;
            }
            let value =
                ((first_remaining & 0x7f) as usize) | (((second_remaining & 0x7f) as usize) << 7);
            if value < 128 {
                break;
            }
            (value, 2usize)
        };
        let total = 1usize + remaining_bytes + remaining_len;
        if total > maximum_packet_size || offset + total > buf.len() {
            break;
        }

        let body_start = offset + 1 + remaining_bytes;
        let body_end = offset + total;
        if body_start + 3 > body_end {
            break;
        }
        let topic_len = u16::from_be_bytes([buf[body_start], buf[body_start + 1]]) as usize;
        if topic_len == 0 {
            break;
        }
        let topic_start = body_start + 2;
        let topic_end = topic_start + topic_len;
        if topic_end >= body_end {
            break;
        }

        let topic_bytes = &buf[topic_start..topic_end];
        // The ASCII case needs no UTF-8 decoder. Non-ASCII is fully legal, but it
        // deliberately falls back to the normal codec + MQTT Unicode validation.
        let mut valid_ascii_topic = true;
        for &byte in topic_bytes {
            if byte >= 0x80 || byte <= 0x1f || byte == 0x7f || byte == b'+' || byte == b'#' {
                valid_ascii_topic = false;
                break;
            }
        }
        if !valid_ascii_topic {
            break;
        }

        // Property Length encoded as the canonical single zero byte. Any property
        // whatsoever (or a non-canonical encoding) is handled by the full codec.
        if buf[topic_end] != 0 {
            break;
        }

        layout.cache_prefix(&buf[offset..=topic_end], total);
        layout.topic = Some(Arc::<str>::from(unsafe {
            std::str::from_utf8_unchecked(topic_bytes)
        }));

        messages += 1;
        offset += total;
    }

    FastIngestBatch {
        bytes: offset,
        messages,
    }
}

/// Scans the MQTT 5 Topic Alias form used by a hot zero-route publisher after the
/// alias mapping has been established on this Network Connection:
/// QoS0, DUP=0, RETAIN=0, zero-length Topic Name, exactly one Topic Alias property.
/// Mapping/range validation is performed once when caching a new alias layout. Any
/// mismatch or unsupported property falls back to the full MQTT codec.
#[inline]
fn scan_zero_route_qos0_alias_batch(
    buf: &[u8],
    maximum_packet_size: usize,
    layout: &mut FastIngestLayout,
    session: &ClientSession,
) -> FastIngestBatch {
    let mut offset = 0usize;
    let mut messages = 0u64;

    while offset < buf.len() {
        if layout.total_len != 0 {
            let total = layout.total_len;
            if layout.scalar9 {
                let sixteen = total.saturating_mul(16);
                while offset + sixteen <= buf.len() {
                    if !fast_ingest_layout_matches_16_scalar9(buf, offset, total, layout) {
                        break;
                    }
                    messages += 16;
                    offset += sixteen;
                }
            }
            let eight = total.saturating_mul(8);
            while offset + eight <= buf.len() {
                if !fast_ingest_layout_matches_8(buf, offset, total, layout) {
                    break;
                }
                messages += 8;
                offset += eight;
            }
            if offset + total > buf.len() {
                break;
            }
            if fast_ingest_layout_matches(buf, offset, layout) {
                messages += 1;
                offset += total;
                continue;
            }
        }

        if buf[offset] != 0x30 || offset + 1 >= buf.len() {
            break;
        }
        let first_remaining = buf[offset + 1];
        let (remaining_len, remaining_bytes) = if first_remaining & 0x80 == 0 {
            (first_remaining as usize, 1usize)
        } else {
            if offset + 2 >= buf.len() {
                break;
            }
            let second_remaining = buf[offset + 2];
            if second_remaining & 0x80 != 0 {
                break;
            }
            let value =
                ((first_remaining & 0x7f) as usize) | (((second_remaining & 0x7f) as usize) << 7);
            if value < 128 {
                break;
            }
            (value, 2usize)
        };
        let total = 1usize + remaining_bytes + remaining_len;
        if total > maximum_packet_size || offset + total > buf.len() {
            break;
        }

        let body_start = offset + 1 + remaining_bytes;
        let body_end = offset + total;
        // Topic length (2) + property length (1) + property id (1) + alias (2).
        if body_start + 6 > body_end {
            break;
        }
        if buf[body_start] != 0 || buf[body_start + 1] != 0 {
            break;
        }
        let prop_start = body_start + 2;
        if buf[prop_start] != 3 || buf[prop_start + 1] != 0x23 {
            break;
        }
        let alias = u16::from_be_bytes([buf[prop_start + 2], buf[prop_start + 3]]);
        if alias == 0 || alias > session.topic_alias_maximum {
            break;
        }
        // This lock is only taken when discovering/changing an alias packet layout.
        // Once cached, the same connection owns the mapping for its lifetime.
        if !session.topic_aliases.read().contains_key(&alias) {
            break;
        }

        let prefix_end = prop_start + 4;
        layout.cache_prefix(&buf[offset..prefix_end], total);
        layout.topic = None;
        layout.alias = Some(alias);
        messages += 1;
        offset += total;
    }

    FastIngestBatch {
        bytes: offset,
        messages,
    }
}

#[cfg(target_arch = "aarch64")]
type FastRouteTimestamp = u64;
#[cfg(not(target_arch = "aarch64"))]
type FastRouteTimestamp = Instant;

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn fast_route_timestamp() -> FastRouteTimestamp {
    let value: u64;
    unsafe {
        core::arch::asm!(
            "mrs {value}, cntvct_el0",
            value = out(reg) value,
            options(nostack, preserves_flags),
        );
    }
    value
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn fast_route_elapsed(start: FastRouteTimestamp) -> Duration {
    let end = fast_route_timestamp();
    let ticks = end.wrapping_sub(start);
    let frequency: u64;
    unsafe {
        core::arch::asm!(
            "mrs {frequency}, cntfrq_el0",
            frequency = out(reg) frequency,
            options(nostack, preserves_flags),
        );
    }
    if frequency == 1_000_000_000 {
        Duration::from_nanos(ticks)
    } else if frequency == 0 {
        Duration::ZERO
    } else {
        let nanos = (ticks as u128)
            .saturating_mul(1_000_000_000)
            .checked_div(frequency as u128)
            .unwrap_or(0)
            .min(u64::MAX as u128) as u64;
        Duration::from_nanos(nanos)
    }
}

#[cfg(not(target_arch = "aarch64"))]
#[inline(always)]
fn fast_route_timestamp() -> FastRouteTimestamp {
    Instant::now()
}

#[cfg(not(target_arch = "aarch64"))]
#[inline(always)]
fn fast_route_elapsed(start: FastRouteTimestamp) -> Duration {
    start.elapsed()
}

#[inline]
fn sampled_sequences_in_range(start: u64, count: u64, sample_rate: u64) -> u64 {
    if count == 0 {
        return 0;
    }
    debug_assert!(sample_rate.is_power_of_two());
    let mask = sample_rate - 1;
    let first_delta = sample_rate.wrapping_sub(start & mask) & mask;
    if first_delta >= count {
        0
    } else {
        1 + (count - 1 - first_delta) / sample_rate
    }
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

            // Topic Alias is Network Connection state in MQTT 5. A non-empty Topic
            // Name + alias creates/replaces a mapping; an empty Topic Name resolves
            // an existing mapping. Alias 0/out-of-range is 0x94, while using an alias
            // before it has a mapping is a Protocol Error (0x82).
            let resolved_topic: Cow<'_, str> = match pkt.properties.topic_alias {
                Some(alias) => {
                    if alias == 0 || alias > session.topic_alias_maximum {
                        return protocol_error(
                            state,
                            session,
                            0x94,
                            "Topic Alias outside the server-advertised range",
                        )
                        .await;
                    }
                    if pkt.topic.is_empty() {
                        let mapped = session.topic_aliases.read().get(&alias).cloned();
                        let Some(mapped) = mapped else {
                            return protocol_error(
                                state,
                                session,
                                0x82,
                                "Topic Alias used before a mapping was established",
                            )
                            .await;
                        };
                        Cow::Owned(mapped)
                    } else {
                        if let Some(reason) = publish_topic_error_reason(pkt.topic) {
                            return protocol_error(
                                state,
                                session,
                                reason,
                                "PUBLISH topic contains MQTT-forbidden characters",
                            )
                            .await;
                        }
                        session.set_inbound_topic_alias(alias, pkt.topic);
                        Cow::Borrowed(pkt.topic)
                    }
                }
                None => {
                    if pkt.topic.is_empty() {
                        return protocol_error(
                            state,
                            session,
                            0x82,
                            "Zero-length PUBLISH topic without Topic Alias",
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
                    Cow::Borrowed(pkt.topic)
                }
            };
            let topic = resolved_topic.as_ref();
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
            if !session.allow_all_write && !state.auth.authorize(username, topic, "write") {
                warn!(
                    "Client '{}' (user: '{}') not authorized to publish on topic '{}'",
                    session.client_id, username, topic
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
                        topic: topic.to_string(),
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
                state.update_retained(topic, pkt.payload, pkt.qos, &application_properties);
            }

            let publish_sequence = session.published_messages.fetch_add(1, Ordering::Relaxed);
            state
                .route_publish(
                    &session.client_id,
                    topic,
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

#[cfg(test)]
mod fast_ingest_tests {
    use super::*;

    #[test]
    fn native_batch_scans_multiple_simple_publishes() {
        let one = crate::codec::encode_publish_qos0("bench/native/1", b"payload", false, None);
        let two = crate::codec::encode_publish_qos0("bench/native/2", b"payload2", false, None);
        let mut joined = one.clone();
        joined.extend_from_slice(&two);
        let batch =
            scan_zero_route_qos0_batch(&joined, 1024 * 1024, &mut FastIngestLayout::default());
        assert_eq!(batch.messages, 2);
        assert_eq!(batch.bytes, joined.len());
    }

    #[test]
    fn native_batch_falls_back_for_non_ascii_or_properties() {
        let non_ascii = crate::codec::encode_publish_qos0("bench/café", b"x", false, None);
        assert_eq!(
            scan_zero_route_qos0_batch(&non_ascii, 1024, &mut FastIngestLayout::default()).messages,
            0
        );

        let packet = Packet::Publish(crate::codec::Publish {
            dup: false,
            qos: 0,
            retain: false,
            topic: "bench/native/props",
            packet_id: None,
            properties: crate::codec::PublishProperties {
                content_type: Some("application/octet-stream"),
                ..Default::default()
            },
            payload: b"x",
        });
        let mut buf = Vec::new();
        encode_packet(&packet, &mut buf);
        assert_eq!(
            scan_zero_route_qos0_batch(&buf, 1024, &mut FastIngestLayout::default()).messages,
            0
        );
    }

    #[test]
    fn native_layout_cache_revalidates_when_header_changes() {
        let one = crate::codec::encode_publish_qos0("bench/native/a", b"payload", false, None);
        let two =
            crate::codec::encode_publish_qos0("bench/native/b", b"different-size", false, None);
        let mut joined = one;
        joined.extend_from_slice(&two);
        let mut layout = FastIngestLayout::default();
        let batch = scan_zero_route_qos0_batch(&joined, 1024 * 1024, &mut layout);
        assert_eq!(batch.messages, 2);
        assert_eq!(batch.bytes, joined.len());
    }

    #[test]
    fn vector_layout_match_masks_payload_and_rejects_header_change() {
        let first = crate::codec::encode_publish_qos0("bench/native/a", &[b'a'; 128], false, None);
        let second = crate::codec::encode_publish_qos0("bench/native/a", &[b'z'; 128], false, None);
        let mut layout = FastIngestLayout::default();
        assert_eq!(
            scan_zero_route_qos0_batch(&first, 1024 * 1024, &mut layout).messages,
            1
        );
        assert!(fast_ingest_layout_matches(&second, 0, &layout));
        let changed_topic =
            crate::codec::encode_publish_qos0("bench/native/b", &[b'z'; 128], false, None);
        assert!(!fast_ingest_layout_matches(&changed_topic, 0, &layout));
    }

    #[test]
    fn vector_layout_match_8_detects_any_header_change() {
        let packet =
            crate::codec::encode_publish_qos0("bench/native/eight", &[b'x'; 128], false, None);
        let mut layout = FastIngestLayout::default();
        assert_eq!(
            scan_zero_route_qos0_batch(&packet, 1024 * 1024, &mut layout).messages,
            1
        );
        let mut eight = Vec::with_capacity(packet.len() * 8);
        for _ in 0..8 {
            eight.extend_from_slice(&packet);
        }
        assert!(fast_ingest_layout_matches_8(
            &eight,
            0,
            packet.len(),
            &layout
        ));
        let second_packet = packet.len();
        let topic_byte = 5usize;
        eight[second_packet + topic_byte] ^= 1;
        assert!(!fast_ingest_layout_matches_8(
            &eight,
            0,
            packet.len(),
            &layout
        ));
    }

    #[test]
    fn alias_batch_requires_established_connection_mapping() {
        let tx = Arc::new(OutboundQueue::new(1));
        let session = ClientSession::new(
            "alias-fast".into(),
            None,
            true,
            true,
            true,
            60,
            u16::MAX,
            268_435_455,
            32,
            0,
            tx,
        );
        let payload = [b'x'; 128];
        let packet = crate::codec::encode_publish_qos0_with_topic_alias("", &payload, 1);
        let mut layout = FastIngestLayout::default();
        assert_eq!(
            scan_zero_route_qos0_alias_batch(&packet, 1024 * 1024, &mut layout, &session).messages,
            0
        );
        session
            .topic_aliases
            .write()
            .insert(1, "bench/alias".into());
        assert_eq!(
            scan_zero_route_qos0_alias_batch(&packet, 1024 * 1024, &mut layout, &session).messages,
            1
        );
    }

    #[test]
    fn alias_scalar9_batch_accepts_variable_payload_and_stops_on_alias_change() {
        let tx = Arc::new(OutboundQueue::new(1));
        let session = ClientSession::new(
            "alias-scalar9".into(),
            None,
            true,
            true,
            true,
            60,
            u16::MAX,
            268_435_455,
            32,
            0,
            tx,
        );
        session
            .topic_aliases
            .write()
            .insert(1, "bench/alias".into());
        let seed = crate::codec::encode_publish_qos0_with_topic_alias("", &[b'x'; 128], 1);
        let mut layout = FastIngestLayout::default();
        assert_eq!(
            scan_zero_route_qos0_alias_batch(&seed, 1024 * 1024, &mut layout, &session).messages,
            1
        );
        assert!(layout.scalar9);

        let mut batch = Vec::with_capacity(seed.len() * 16);
        for lane in 0..16u8 {
            let mut payload = [b'x'; 128];
            payload[0] = lane;
            batch.extend_from_slice(&crate::codec::encode_publish_qos0_with_topic_alias(
                "", &payload, 1,
            ));
        }
        let accepted = scan_zero_route_qos0_alias_batch(&batch, 1024 * 1024, &mut layout, &session);
        assert_eq!(accepted.messages, 16);
        assert_eq!(accepted.bytes, batch.len());

        let packet_len = seed.len();
        batch[7 * packet_len + 8] = 2;
        let stopped = scan_zero_route_qos0_alias_batch(&batch, 1024 * 1024, &mut layout, &session);
        assert_eq!(stopped.messages, 7);
        assert_eq!(stopped.bytes, 7 * packet_len);
    }

    #[test]
    fn alias_fast_egress_expands_to_full_topic_and_preserves_variable_payloads() {
        let tx = Arc::new(OutboundQueue::new(1));
        let session = ClientSession::new(
            "alias-egress".into(),
            None,
            true,
            true,
            true,
            60,
            u16::MAX,
            268_435_455,
            32,
            0,
            tx,
        );
        let topic = "bench/native/alias-egress";
        session.topic_aliases.write().insert(1, topic.into());

        let mut input = Vec::new();
        let mut expected = Vec::new();
        for lane in 0..16u8 {
            let mut payload = [b'x'; 128];
            payload[0] = lane;
            input.extend_from_slice(&crate::codec::encode_publish_qos0_with_topic_alias(
                "", &payload, 1,
            ));
            expected.extend_from_slice(&crate::codec::encode_publish_qos0(
                topic, &payload, false, None,
            ));
        }

        let mut layout = FastIngestLayout::default();
        let batch = scan_zero_route_qos0_alias_batch(&input, 1024 * 1024, &mut layout, &session);
        assert_eq!(batch.messages, 16);
        assert_eq!(layout.alias, Some(1));
        let resolved = Arc::<str>::from(topic);
        let mut cache = FastAliasEgressCache::default();
        let (expanded, packet_len) =
            expand_fast_alias_batch(&input, batch, &layout, &resolved, &mut cache).unwrap();
        assert_eq!(expanded.as_ref(), expected.as_slice());
        assert_eq!(packet_len * 16, expected.len());
    }

    #[test]
    fn alias_rewrite_changes_only_alias_and_preserves_payloads() {
        let tx = Arc::new(OutboundQueue::new(1));
        let session = ClientSession::new(
            "alias-rewrite".into(),
            None,
            true,
            true,
            true,
            60,
            u16::MAX,
            268_435_455,
            32,
            0,
            tx,
        );
        session
            .topic_aliases
            .write()
            .insert(1, "bench/rewrite".into());
        let mut input = Vec::new();
        let mut expected = Vec::new();
        for lane in 0..16u8 {
            let mut payload = [b'x'; 128];
            payload[0] = lane;
            input.extend_from_slice(&crate::codec::encode_publish_qos0_with_topic_alias(
                "", &payload, 1,
            ));
            expected.extend_from_slice(&crate::codec::encode_publish_qos0_with_topic_alias(
                "", &payload, 2,
            ));
        }
        let mut layout = FastIngestLayout::default();
        let batch = scan_zero_route_qos0_alias_batch(&input, 1024 * 1024, &mut layout, &session);
        assert_eq!(batch.messages, 16);
        let rewritten = rewrite_fast_alias_batch(&input, batch, &layout, 2).unwrap();
        assert_eq!(rewritten.as_ref(), expected.as_slice());
    }

    #[test]
    fn sample_counter_matches_scalar_rule() {
        for start in [0_u64, 1, 63, 64, 65, 127, 1024] {
            for count in [0_u64, 1, 2, 63, 64, 65, 130] {
                let expected = (start..start + count)
                    .filter(|sequence| sequence & 63 == 0)
                    .count() as u64;
                assert_eq!(sampled_sequences_in_range(start, count, 64), expected);
            }
        }
    }
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
