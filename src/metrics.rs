use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{info, error, debug};
use crate::session::BrokerState;

/// Starts a minimal HTTP server on the given port to serve Prometheus metrics.
/// This implementation has zero HTTP library dependencies to keep the binary light.
pub async fn start_metrics_server(port: u16, state: Arc<BrokerState>) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind metrics server to {}: {:?}", addr, e);
            return;
        }
    };
    info!("Prometheus metrics exporter listening on: http://{}", addr);

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut socket, _addr)) => {
                    let state = state.clone();
                    tokio::spawn(async move {
                        // Wait for socket to be readable to swallow the request
                        if socket.readable().await.is_ok() {
                            let active_connections = state.sessions.read().len();
                            let messages_published = state.metrics_messages_published.load(Ordering::Relaxed);
                            let subscriptions = state.metrics_subscriptions.load(Ordering::Relaxed);

                            let body = format!(
                                "# HELP pipistrelle_connections_total Total number of active client connections\n\
                                 # TYPE pipistrelle_connections_total gauge\n\
                                 pipistrelle_connections_total {}\n\
                                 # HELP pipistrelle_messages_published_total Total number of messages published\n\
                                 # TYPE pipistrelle_messages_published_total counter\n\
                                 pipistrelle_messages_published_total {}\n\
                                 # HELP pipistrelle_subscriptions_total Total number of subscriptions active/added\n\
                                 # TYPE pipistrelle_subscriptions_total counter\n\
                                 pipistrelle_subscriptions_total {}\n",
                                active_connections,
                                messages_published,
                                subscriptions
                            );

                            let response = format!(
                                "HTTP/1.1 200 OK\r\n\
                                 Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                                 Content-Length: {}\r\n\
                                 Connection: close\r\n\
                                 \r\n\
                                 {}",
                                body.len(),
                                body
                            );

                            let _ = socket.write_all(response.as_bytes()).await;
                            let _ = socket.flush().await;
                        }
                    });
                }
                Err(e) => {
                    debug!("Failed to accept metrics connection: {:?}", e);
                }
            }
        }
    });
}
