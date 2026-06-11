mod codec;
mod router;
mod session;
mod config;
mod persistence;
mod tls;
mod websocket;
mod metrics;
mod bridge;







use std::fs::File;
use std::io::{self, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug, Level};
use tracing_subscriber::FmtSubscriber;
use bytes::{BytesMut, Buf};

use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

use crate::codec::{Packet, decode_packet, encode_packet, ConnAck, SubAck, PubAck};
use crate::session::{BrokerState, ClientSession};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize high-performance logging subscriber
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Pipistrelle MQTT v5.0 Broker...");

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
                // Configure ServerConfig with the aws-lc-rs crypto provider
                let provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
                let server_config = ServerConfig::builder_with_provider(Arc::new(provider))
                    .with_safe_default_protocol_versions()
                    .unwrap()
                    .with_no_client_auth()
                    .with_single_cert(certs, key)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                
                info!("Post-Quantum TLS 1.3 encryption engine initialized (using AWS-LC-RS / ML-KEM)");
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
                                    if let Err(e) = handle_connection(tls_stream, addr, state).await {
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
    broker_state.graceful_shutdown();

    // Let connection channels flush before exiting
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("Pipistrelle Broker shutdown complete.");
    Ok(())
}

async fn handle_connection<S>(
    mut socket: S,
    addr: SocketAddr,
    state: Arc<BrokerState>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    debug!("New connection from: {}", addr);

    let mut read_buf = BytesMut::with_capacity(4096);
    
    // 1. Wait for the CONNECT packet first.
    // It must be the first packet received within a reasonable time (e.g., 5 seconds).
    let (client_id, keep_alive, clean_start, session_expiry_interval, username, password) = match tokio::time::timeout(Duration::from_secs(5), socket.read_buf(&mut read_buf)).await {
        Ok(Ok(n)) if n > 0 => {
            match decode_packet(&read_buf) {
                Ok((Packet::Connect(pkt), bytes_read)) => {
                    let client_id = pkt.client_id.to_string();
                    let keep_alive = pkt.keep_alive;
                    let clean_start = pkt.clean_start;
                    let session_expiry_interval = pkt.properties.session_expiry_interval.unwrap_or(0);
                    let username = pkt.username.map(|s| s.to_string());
                    let password = pkt.password.map(|b| String::from_utf8_lossy(b).into_owned());
                    read_buf.advance(bytes_read);
                    (client_id, keep_alive, clean_start, session_expiry_interval, username, password)
                }
                Ok((other, _)) => {
                    warn!("First packet was not CONNECT: {:?}", other);
                    return Ok(());
                }
                Err(e) => {
                    warn!("Failed to decode CONNECT packet: {:?}", e);
                    return Ok(());
                }
            }
        }
        _ => {
            warn!("Timeout or connection closed before CONNECT received");
            return Ok(());
        }
    };
    
    // Authenticate client
    let authenticated = match (&username, &password) {
        (Some(u), Some(p)) => state.auth.authenticate(u, p),
        (None, None) => state.auth.authenticate("", ""), // Try anonymous access
        _ => false, // Missing either username or password when the other is present
    };

    if !authenticated {
        warn!("Authentication failed for client '{}' (username: {:?})", client_id, username);
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
    
    info!("Client '{}' (authenticated user: {:?}) connecting from {}", client_id, username, addr);

    // 2. Set up channels for sending outgoing packets to this client
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    
    let session = Arc::new(ClientSession::new(
        client_id.clone(),
        username,
        clean_start,
        keep_alive,
        session_expiry_interval,
        tx,
    ));

    state.register_session(session.clone());

    // 3. Spawn a dedicated TCP writer task for this client
    let (mut read_half, mut write_half) = tokio::io::split(socket);
    let client_id_clone = client_id.clone();
    
    let writer_task = tokio::spawn(async move {
        while let Some(bytes) = rx.recv().await {
            if let Err(e) = write_half.write_all(&bytes).await {
                warn!("Failed to write to client {}: {:?}", client_id_clone, e);
                break;
            }
            if let Err(e) = write_half.flush().await {
                warn!("Failed to flush client {}: {:?}", client_id_clone, e);
                break;
            }
        }
        // Ensure socket write half is closed
        let _ = write_half.shutdown().await;
        debug!("Writer task terminated for client {}", client_id_clone);
    });

    // 4. Send CONNACK response
    let connack = Packet::ConnAck(ConnAck {
        session_present: false,
        reason_code: 0, // Success
        properties: Default::default(),
    });
    let mut connack_buf = Vec::new();
    encode_packet(&connack, &mut connack_buf);
    let _ = session.sender.send(connack_buf);

    // 5. Main packet reading and processing loop
    let keep_alive_duration = if keep_alive > 0 {
        // MQTT spec recommends 1.5 times the keep alive time
        Duration::from_millis((keep_alive as u64 * 1500) as u64)
    } else {
        Duration::from_secs(3600 * 24) // Extremely large timeout if keep alive is 0
    };

    let result: Result<(), Box<dyn std::error::Error>> = async {
        loop {
            // Read next bytes with a timeout based on keep-alive
            let read_result = tokio::time::timeout(
                keep_alive_duration,
                read_half.read_buf(&mut read_buf)
            ).await;

            match read_result {
                Ok(Ok(0)) => {
                    info!("Client '{}' closed connection", client_id);
                    break;
                }
                Ok(Ok(_)) => {
                    // Update session keep-alive timer
                    session.update_activity();

                    // Parse and process all complete packets currently in the read buffer
                    loop {
                        match decode_packet(&read_buf) {
                            Ok((packet, bytes_read)) => {
                                process_client_packet(&packet, &state, &session)?;
                                read_buf.advance(bytes_read);
                            }
                            Err(codec::CodecError::Incomplete) => {
                                // Wait for more data
                                break;
                            }
                            Err(e) => {
                                warn!("Codec error processing client '{}': {:?}", client_id, e);
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
    }.await;

    // 6. Cleanup session on disconnect or error
    state.remove_session(&client_id);
    writer_task.abort(); // Cancel writer task

    result
}

fn process_client_packet(
    packet: &Packet<'_>,
    state: &BrokerState,
    session: &ClientSession,
) -> Result<(), Box<dyn std::error::Error>> {
    match packet {
        Packet::Publish(pkt) => {
            debug!("Received PUBLISH from client '{}' on topic '{}'", session.client_id, pkt.topic);
            
            // Check write authorization
            let username = session.username.as_deref().unwrap_or("");
            if !state.auth.authorize(username, pkt.topic, "write") {
                warn!("Client '{}' (user: '{}') not authorized to publish on topic '{}'", session.client_id, username, pkt.topic);
                if pkt.qos == 1 {
                    if let Some(pid) = pkt.packet_id {
                        let puback = Packet::PubAck(PubAck {
                            packet_id: pid,
                            reason_code: 0x87, // Not Authorized
                            properties: Default::default(),
                        });
                        let mut buf = Vec::new();
                        encode_packet(&puback, &mut buf);
                        let _ = session.sender.send(buf);
                    }
                }
                return Ok(());
            }

            state.route_publish(&session.client_id, pkt.topic, pkt.payload, pkt.qos, pkt.retain);
            
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
                    let _ = session.sender.send(buf);
                }
            }
        }
        Packet::Subscribe(pkt) => {
            debug!("Received SUBSCRIBE from client '{}'", session.client_id);
            let mut reason_codes = Vec::new();
            let username = session.username.as_deref().unwrap_or("");
            
            for sub in &pkt.subscriptions {
                // Check read authorization
                if state.auth.authorize(username, sub.topic_filter, "read") {
                    state.subscribe(&session.client_id, sub.topic_filter, sub.options & 0x03, pkt.properties.subscription_identifier);
                    reason_codes.push(sub.options & 0x03);
                } else {
                    warn!("Client '{}' (user: '{}') not authorized to subscribe to filter '{}'", session.client_id, username, sub.topic_filter);
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
            let _ = session.sender.send(buf);
        }
        Packet::PubAck(pkt) => {
            debug!("Received PUBACK from client '{}' for packet ID {}", session.client_id, pkt.packet_id);
            session.remove_in_flight(pkt.packet_id);
            
            // Delete from database
            let db = state.db.clone();
            let cid = session.client_id.clone();
            let pid = pkt.packet_id;
            tokio::spawn(async move {
                db.delete_in_flight(cid, pid).await;
            });
        }
        Packet::PingReq => {
            debug!("Received PINGREQ from client '{}'", session.client_id);
            let pingresp = Packet::PingResp;
            let mut buf = Vec::new();
            encode_packet(&pingresp, &mut buf);
            let _ = session.sender.send(buf);
        }
        Packet::Disconnect(_) => {
            info!("Received DISCONNECT from client '{}'", session.client_id);
            // The read loop will naturally exit because we stop processing
        }
        Packet::Connect(_) => {
            warn!("Client '{}' sent CONNECT packet mid-session, violating protocol", session.client_id);
            return Err("Protocol violation: duplicate CONNECT".into());
        }
        other => {
            warn!("Unsupported packet type from client '{}': {:?}", session.client_id, other);
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
