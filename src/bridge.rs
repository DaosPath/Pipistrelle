use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{info, error, debug};
use bytes::{BytesMut, Buf};

use crate::codec::{Packet, Connect, Subscribe, Subscription, decode_packet, encode_packet};
use crate::session::BrokerState;

pub async fn start_bridge_engine(state: Arc<BrokerState>) {
    let host = match std::env::var("PIPISTRELLE_BRIDGE_HOST") {
        Ok(h) => h,
        Err(_) => {
            info!("PIPISTRELLE_BRIDGE_HOST not set. MQTT bridging is disabled.");
            return;
        }
    };

    let user = std::env::var("PIPISTRELLE_BRIDGE_USER").unwrap_or_default();
    let pass = std::env::var("PIPISTRELLE_BRIDGE_PASS").unwrap_or_default();
    let port = std::env::var("PIPISTRELLE_BRIDGE_PORT")
        .unwrap_or_else(|_| "8883".to_string())
        .parse::<u16>()
        .unwrap_or(8883);

    // Topic configurations (for bi-directional bridge)
    // We will publish local "sensor/#" messages to HiveMQ Cloud,
    // and subscribe to cloud "alerts/#" messages to route them locally.
    let local_pub_pattern = "sensor/";
    let remote_sub_filter = "alerts/#";

    info!("Starting MQTT Bridging engine to HiveMQ Cloud ({}:{})", host, port);

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Vec<u8>)>();
    *state.bridge_sender.write() = Some(tx);

    let state_clone = state.clone();
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(5);
        
        loop {
            info!("Bridge attempting to connect to remote broker at {}:{}...", host, port);
            match connect_and_run_bridge(
                &host,
                port,
                &user,
                &pass,
                remote_sub_filter,
                local_pub_pattern,
                state_clone.clone(),
                &mut rx,
            ).await {
                Ok(_) => {
                    info!("Bridge connection closed cleanly. Reconnecting in 5s...");
                    backoff = Duration::from_secs(5);
                }
                Err(e) => {
                    let err_msg = format!("{:?}", e);
                    error!("Bridge error: {}. Retrying in {}s...", err_msg, backoff.as_secs());
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, Duration::from_secs(60));
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn connect_and_run_bridge(
    host: &str,
    port: u16,
    user: &str,
    pass: &str,
    remote_sub_filter: &str,
    local_pub_pattern: &str,
    state: Arc<BrokerState>,
    local_rx: &mut mpsc::UnboundedReceiver<(String, Vec<u8>)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Establish TCP connection
    let addr = format!("{}:{}", host, port);
    let tcp_stream = TcpStream::connect(&addr).await?;
    debug!("Bridge TCP connection established with {}", addr);

    // 2. Configure TLS to trust public roots
    let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
    root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    // Use default provider
    let provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
    let client_config = tokio_rustls::rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let server_name = rustls_pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    let mut tls_stream = connector.connect(server_name, tcp_stream).await?;
    info!("Bridge secure TLS connection completed.");

    // 3. Send CONNECT packet
    let client_id = "pipistrelle_bridge_client";
    let connect_pkt = Packet::Connect(Connect {
        client_id,
        keep_alive: 60,
        clean_start: true,
        username: if !user.is_empty() { Some(user) } else { None },
        password: if !pass.is_empty() { Some(pass.as_bytes()) } else { None },
        properties: Default::default(),
    });

    let mut connect_buf = Vec::new();
    encode_packet(&connect_pkt, &mut connect_buf);
    tls_stream.write_all(&connect_buf).await?;
    tls_stream.flush().await?;

    // 4. Wait for CONNACK
    let mut read_buf = BytesMut::with_capacity(4096);
    let n = tokio::time::timeout(Duration::from_secs(10), tls_stream.read_buf(&mut read_buf)).await??;
    if n == 0 {
        return Err("Connection closed before CONNACK received".into());
    }

    match decode_packet(&read_buf) {
        Ok((Packet::ConnAck(connack), bytes_read)) => {
            if connack.reason_code != 0 {
                return Err(format!("Bridge connection rejected: code {}", connack.reason_code).into());
            }
            read_buf.advance(bytes_read);
            info!("Bridge successfully authenticated with HiveMQ Cloud.");
        }
        _ => return Err("Expected CONNACK packet".into()),
    }

    // 5. Send SUBSCRIBE to remote broker
    let subscribe_pkt = Packet::Subscribe(Subscribe {
        packet_id: 1,
        properties: Default::default(),
        subscriptions: vec![Subscription {
            topic_filter: remote_sub_filter,
            options: 0, // QoS 0
        }],
    });

    let mut sub_buf = Vec::new();
    encode_packet(&subscribe_pkt, &mut sub_buf);
    tls_stream.write_all(&sub_buf).await?;
    tls_stream.flush().await?;

    // 6. Split stream into reader and writer halves
    let (mut read_half, mut write_half) = tokio::io::split(tls_stream);

    // 7. Start ping loop to keep remote connection alive
    let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(20)).await;
            if ping_tx.send(()).await.is_err() {
                break;
            }
        }
    });

    // 8. Main bridging loop
    loop {
        tokio::select! {
            // Read remote publish messages and route them locally
            read_res = read_half.read_buf(&mut read_buf) => {
                let bytes_read = read_res?;
                if bytes_read == 0 {
                    return Err("Remote broker closed bridge connection".into());
                }

                loop {
                    match decode_packet(&read_buf) {
                        Ok((Packet::Publish(pkt), consumed)) => {
                            debug!("Bridge received remote publish: topic='{}'", pkt.topic);
                            // Route locally (prevent loops by using special sender name)
                            state.route_publish("bridge_client", pkt.topic, pkt.payload, pkt.qos, pkt.retain);
                            read_buf.advance(consumed);
                        }
                        Ok((Packet::PingResp, consumed)) => {
                            debug!("Bridge received PINGRESP");
                            read_buf.advance(consumed);
                        }
                        Ok((Packet::SubAck(_), consumed)) => {
                            debug!("Bridge received SUBACK");
                            read_buf.advance(consumed);
                        }
                        Ok((_, consumed)) => {
                            read_buf.advance(consumed);
                        }
                        Err(crate::codec::CodecError::Incomplete) => {
                            break;
                        }
                        Err(e) => {
                            return Err(format!("Bridge decode error: {:?}", e).into());
                        }
                    }
                }
            }
            // Read local publishes from channel and forward to remote broker
            local_msg = local_rx.recv() => {
                if let Some((topic, payload)) = local_msg {
                    // Only bridge if it matches the configured prefix
                    if topic.starts_with(local_pub_pattern) {
                        debug!("Bridge forwarding local publish to remote: '{}'", topic);
                        let pub_pkt = Packet::Publish(crate::codec::Publish {
                            dup: false,
                            qos: 0,
                            retain: false,
                            topic: &topic,
                            packet_id: None,
                            properties: Default::default(),
                            payload: &payload,
                        });

                        let mut buf = Vec::new();
                        encode_packet(&pub_pkt, &mut buf);
                        write_half.write_all(&buf).await?;
                        write_half.flush().await?;
                    }
                }
            }
            // Send PINGREQ occasionally
            _ = ping_rx.recv() => {
                debug!("Bridge sending PINGREQ");
                let ping_pkt = Packet::PingReq;
                let mut buf = Vec::new();
                encode_packet(&ping_pkt, &mut buf);
                write_half.write_all(&buf).await?;
                write_half.flush().await?;
            }
        }
    }
}
