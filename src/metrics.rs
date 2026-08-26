use crate::crypto::TlsProfile;
use crate::session::BrokerState;
use crate::version;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, info};

/// Lightweight observability HTTP server without an HTTP framework dependency.
pub async fn start_metrics_server(port: u16, state: Arc<BrokerState>) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind observability server to {}: {:?}", addr, e);
            return;
        }
    };
    info!("Observability server listening on: http://{}", addr);

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut socket, _addr)) => {
                    let state = state.clone();
                    tokio::spawn(async move {
                        let mut request = [0_u8; 2048];
                        let Ok(n) = socket.read(&mut request).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }

                        let request = String::from_utf8_lossy(&request[..n]);
                        let path = request
                            .lines()
                            .next()
                            .and_then(|line| line.split_whitespace().nth(1))
                            .unwrap_or("/");

                        let tls_profile =
                            TlsProfile::from_env("PIPISTRELLE_TLS_PROFILE", TlsProfile::Hybrid);

                        let (status, content_type, body) = match path {
                            "/metrics" => {
                                let active_connections = state.sessions.read().len();
                                let messages_published =
                                    state.metrics_messages_published.load(Ordering::Relaxed);
                                let subscriptions =
                                    state.metrics_subscriptions.load(Ordering::Relaxed);
                                let tls_pqc =
                                    state.metrics_tls_pqc_handshakes.load(Ordering::Relaxed);
                                let tls_classical = state
                                    .metrics_tls_classical_handshakes
                                    .load(Ordering::Relaxed);
                                let body = format!(
                                    concat!(
                                        "# HELP pipistrelle_connections_total Active MQTT client connections\n",
                                        "# TYPE pipistrelle_connections_total gauge\n",
                                        "pipistrelle_connections_total {}\n",
                                        "# HELP pipistrelle_messages_published_total Messages published since startup\n",
                                        "# TYPE pipistrelle_messages_published_total counter\n",
                                        "pipistrelle_messages_published_total {}\n",
                                        "# HELP pipistrelle_subscriptions_total Subscriptions added since startup\n",
                                        "# TYPE pipistrelle_subscriptions_total counter\n",
                                        "pipistrelle_subscriptions_total {}\n",
                                        "# HELP pipistrelle_tls_handshakes_total Successful TLS 1.3 handshakes by negotiated key exchange family\n",
                                        "# TYPE pipistrelle_tls_handshakes_total counter\n",
                                        "pipistrelle_tls_handshakes_total{{kind=\"pqc\"}} {}\n",
                                        "pipistrelle_tls_handshakes_total{{kind=\"classical\"}} {}\n",
                                        "# HELP pipistrelle_build_info Pipistrelle build information\n",
                                        "# TYPE pipistrelle_build_info gauge\n",
                                        "pipistrelle_build_info{{version=\"{}\",series=\"{}\",tls_profile=\"{}\"}} 1\n",
                                    ),
                                    active_connections,
                                    messages_published,
                                    subscriptions,
                                    tls_pqc,
                                    tls_classical,
                                    version::VERSION,
                                    version::SERIES,
                                    tls_profile.as_str(),
                                );
                                ("200 OK", "text/plain; version=0.0.4; charset=utf-8", body)
                            }
                            "/health" => (
                                "200 OK",
                                "application/json; charset=utf-8",
                                format!(
                                    "{{\"status\":\"ok\",\"version\":\"{}\",\"series\":\"{}\"}}\n",
                                    version::VERSION,
                                    version::SERIES,
                                ),
                            ),
                            "/info" => (
                                "200 OK",
                                "application/json; charset=utf-8",
                                format!(
                                    "{{\"name\":\"pipistrelle\",\"version\":\"{}\",\"series\":\"{}\",\"mqtt\":\"5.0\",\"tls\":\"1.3\",\"tls_profile\":\"{}\",\"pqc_kx\":\"X25519MLKEM768\"}}\n",
                                    version::VERSION,
                                    version::SERIES,
                                    tls_profile.as_str(),
                                ),
                            ),
                            _ => (
                                "404 Not Found",
                                "text/plain; charset=utf-8",
                                "not found\n".to_string(),
                            ),
                        };

                        let response = format!(
                            "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            status,
                            content_type,
                            body.len(),
                            body,
                        );
                        let _ = socket.write_all(response.as_bytes()).await;
                        let _ = socket.flush().await;
                    });
                }
                Err(e) => debug!("Failed to accept observability connection: {:?}", e),
            }
        }
    });
}
