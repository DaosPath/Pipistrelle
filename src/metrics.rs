use crate::crypto::TlsProfile;
use crate::session::BrokerState;
use crate::version;
use std::fmt::Write as _;
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
                                let (
                                    active_connections,
                                    queued_messages,
                                    queue_capacity_total,
                                    current_subscriptions,
                                    active_published_messages,
                                ) = {
                                    let sessions = state.sessions.read();
                                    let queued_messages = sessions
                                        .values()
                                        .map(|session| {
                                            session
                                                .sender
                                                .max_capacity()
                                                .saturating_sub(session.sender.capacity())
                                        })
                                        .sum::<usize>();
                                    let queue_capacity_total = sessions
                                        .values()
                                        .map(|session| session.sender.max_capacity())
                                        .sum::<usize>();
                                    let current_subscriptions = sessions
                                        .values()
                                        .map(|session| session.subscription_count())
                                        .sum::<usize>();
                                    let active_published_messages = sessions
                                        .values()
                                        .map(|session| {
                                            session.published_messages.load(Ordering::Relaxed)
                                        })
                                        .sum::<u64>();
                                    (
                                        sessions.len(),
                                        queued_messages,
                                        queue_capacity_total,
                                        current_subscriptions,
                                        active_published_messages,
                                    )
                                };
                                let (bridge_queued_messages, bridge_queue_capacity) = state
                                    .bridge_sender
                                    .read()
                                    .as_ref()
                                    .map(|bridge| {
                                        (
                                            bridge
                                                .sender
                                                .max_capacity()
                                                .saturating_sub(bridge.sender.capacity()),
                                            bridge.sender.max_capacity(),
                                        )
                                    })
                                    .unwrap_or((0, 0));

                                let messages_published = state
                                    .metrics_messages_published_retired
                                    .load(Ordering::Relaxed)
                                    .saturating_add(active_published_messages);
                                let subscriptions =
                                    state.metrics_subscriptions.load(Ordering::Relaxed);
                                let tls_pqc =
                                    state.metrics_tls_pqc_handshakes.load(Ordering::Relaxed);
                                let tls_classical = state
                                    .metrics_tls_classical_handshakes
                                    .load(Ordering::Relaxed);
                                let queue_backpressure_events = state
                                    .metrics_client_queue_backpressure_events
                                    .load(Ordering::Relaxed);
                                let queue_backpressure_wait_seconds = state
                                    .metrics_client_queue_backpressure_wait_ns
                                    .load(Ordering::Relaxed)
                                    as f64
                                    / 1_000_000_000.0;
                                let slow_consumer_disconnects = state
                                    .metrics_slow_consumer_disconnects
                                    .load(Ordering::Relaxed);
                                let subscription_quota_rejections = state
                                    .metrics_subscription_quota_rejections
                                    .load(Ordering::Relaxed);
                                let bridge_backpressure_events = state
                                    .metrics_bridge_queue_backpressure_events
                                    .load(Ordering::Relaxed);
                                let bridge_backpressure_wait_seconds = state
                                    .metrics_bridge_queue_backpressure_wait_ns
                                    .load(Ordering::Relaxed)
                                    as f64
                                    / 1_000_000_000.0;
                                let bridge_dropped =
                                    state.metrics_bridge_queue_dropped.load(Ordering::Relaxed);

                                let mut body = String::with_capacity(8192);
                                macro_rules! metric {
                                    ($($arg:tt)*) => {{
                                        let _ = writeln!(&mut body, $($arg)*);
                                    }};
                                }

                                metric!(
                                    "# HELP pipistrelle_connections_total Active MQTT client connections"
                                );
                                metric!("# TYPE pipistrelle_connections_total gauge");
                                metric!("pipistrelle_connections_total {}", active_connections);
                                metric!(
                                    "# HELP pipistrelle_messages_published_total Messages published since startup"
                                );
                                metric!("# TYPE pipistrelle_messages_published_total counter");
                                metric!(
                                    "pipistrelle_messages_published_total {}",
                                    messages_published
                                );
                                metric!(
                                    "# HELP pipistrelle_subscriptions_total Subscriptions added since startup"
                                );
                                metric!("# TYPE pipistrelle_subscriptions_total counter");
                                metric!("pipistrelle_subscriptions_total {}", subscriptions);
                                metric!(
                                    "# HELP pipistrelle_client_subscriptions_current Current subscriptions tracked across connected sessions"
                                );
                                metric!("# TYPE pipistrelle_client_subscriptions_current gauge");
                                metric!(
                                    "pipistrelle_client_subscriptions_current {}",
                                    current_subscriptions
                                );
                                metric!(
                                    "# HELP pipistrelle_tls_handshakes_total Successful TLS 1.3 handshakes by negotiated key exchange family"
                                );
                                metric!("# TYPE pipistrelle_tls_handshakes_total counter");
                                metric!(
                                    "pipistrelle_tls_handshakes_total{{kind=\"pqc\"}} {}",
                                    tls_pqc
                                );
                                metric!(
                                    "pipistrelle_tls_handshakes_total{{kind=\"classical\"}} {}",
                                    tls_classical
                                );

                                metric!(
                                    "# HELP pipistrelle_client_outbound_queue_messages Messages currently buffered across client outbound queues"
                                );
                                metric!("# TYPE pipistrelle_client_outbound_queue_messages gauge");
                                metric!(
                                    "pipistrelle_client_outbound_queue_messages {}",
                                    queued_messages
                                );
                                metric!(
                                    "# HELP pipistrelle_client_outbound_queue_capacity_messages Total bounded outbound queue capacity across connected clients"
                                );
                                metric!(
                                    "# TYPE pipistrelle_client_outbound_queue_capacity_messages gauge"
                                );
                                metric!(
                                    "pipistrelle_client_outbound_queue_capacity_messages {}",
                                    queue_capacity_total
                                );
                                metric!(
                                    "# HELP pipistrelle_client_queue_backpressure_total Number of times a client outbound queue filled and forced producer backpressure"
                                );
                                metric!(
                                    "# TYPE pipistrelle_client_queue_backpressure_total counter"
                                );
                                metric!(
                                    "pipistrelle_client_queue_backpressure_total {}",
                                    queue_backpressure_events
                                );
                                metric!(
                                    "# HELP pipistrelle_client_queue_backpressure_wait_seconds_total Cumulative time producers waited for bounded client queue capacity"
                                );
                                metric!(
                                    "# TYPE pipistrelle_client_queue_backpressure_wait_seconds_total counter"
                                );
                                metric!(
                                    "pipistrelle_client_queue_backpressure_wait_seconds_total {:.9}",
                                    queue_backpressure_wait_seconds
                                );
                                metric!(
                                    "# HELP pipistrelle_slow_consumer_disconnects_total Clients disconnected by the slow-consumer policy"
                                );
                                metric!(
                                    "# TYPE pipistrelle_slow_consumer_disconnects_total counter"
                                );
                                metric!(
                                    "pipistrelle_slow_consumer_disconnects_total {}",
                                    slow_consumer_disconnects
                                );
                                metric!(
                                    "# HELP pipistrelle_subscription_quota_rejections_total SUBSCRIBE filters rejected because a client exceeded its quota"
                                );
                                metric!(
                                    "# TYPE pipistrelle_subscription_quota_rejections_total counter"
                                );
                                metric!(
                                    "pipistrelle_subscription_quota_rejections_total {}",
                                    subscription_quota_rejections
                                );

                                metric!(
                                    "# HELP pipistrelle_bridge_outbound_queue_messages Messages waiting for the remote bridge"
                                );
                                metric!("# TYPE pipistrelle_bridge_outbound_queue_messages gauge");
                                metric!(
                                    "pipistrelle_bridge_outbound_queue_messages {}",
                                    bridge_queued_messages
                                );
                                metric!(
                                    "# HELP pipistrelle_bridge_outbound_queue_capacity_messages Bounded remote bridge queue capacity"
                                );
                                metric!(
                                    "# TYPE pipistrelle_bridge_outbound_queue_capacity_messages gauge"
                                );
                                metric!(
                                    "pipistrelle_bridge_outbound_queue_capacity_messages {}",
                                    bridge_queue_capacity
                                );
                                metric!(
                                    "# HELP pipistrelle_bridge_queue_backpressure_total Times the remote bridge queue reached capacity"
                                );
                                metric!(
                                    "# TYPE pipistrelle_bridge_queue_backpressure_total counter"
                                );
                                metric!(
                                    "pipistrelle_bridge_queue_backpressure_total {}",
                                    bridge_backpressure_events
                                );
                                metric!(
                                    "# HELP pipistrelle_bridge_queue_backpressure_wait_seconds_total Cumulative bridge producer wait time when policy is backpressure"
                                );
                                metric!(
                                    "# TYPE pipistrelle_bridge_queue_backpressure_wait_seconds_total counter"
                                );
                                metric!(
                                    "pipistrelle_bridge_queue_backpressure_wait_seconds_total {:.9}",
                                    bridge_backpressure_wait_seconds
                                );
                                metric!(
                                    "# HELP pipistrelle_bridge_queue_dropped_total Bridge messages dropped by the explicit drop-newest policy"
                                );
                                metric!("# TYPE pipistrelle_bridge_queue_dropped_total counter");
                                metric!(
                                    "pipistrelle_bridge_queue_dropped_total {}",
                                    bridge_dropped
                                );

                                metric!(
                                    "# HELP pipistrelle_publish_route_latency_sample_rate One latency observation is recorded for every N publishes"
                                );
                                metric!(
                                    "# TYPE pipistrelle_publish_route_latency_sample_rate gauge"
                                );
                                metric!(
                                    "pipistrelle_publish_route_latency_sample_rate {}",
                                    state.publish_route_latency_sample_rate
                                );
                                metric!(
                                    "# HELP pipistrelle_publish_route_latency_seconds Sampled time spent routing one inbound PUBLISH, including bounded queue waits"
                                );
                                metric!(
                                    "# TYPE pipistrelle_publish_route_latency_seconds histogram"
                                );
                                for (bound_us, cumulative) in
                                    state.publish_route_latency.cumulative_buckets()
                                {
                                    match bound_us {
                                        Some(bound_us) => metric!(
                                            "pipistrelle_publish_route_latency_seconds_bucket{{le=\"{:.6}\"}} {}",
                                            bound_us as f64 / 1_000_000.0,
                                            cumulative
                                        ),
                                        None => metric!(
                                            "pipistrelle_publish_route_latency_seconds_bucket{{le=\"+Inf\"}} {}",
                                            cumulative
                                        ),
                                    }
                                }
                                metric!(
                                    "pipistrelle_publish_route_latency_seconds_count {}",
                                    state.publish_route_latency.count()
                                );
                                metric!(
                                    "pipistrelle_publish_route_latency_seconds_sum {:.9}",
                                    state.publish_route_latency.sum_seconds()
                                );
                                metric!(
                                    "# HELP pipistrelle_publish_route_latency_p50_seconds Approximate P50 publish routing latency"
                                );
                                metric!(
                                    "# TYPE pipistrelle_publish_route_latency_p50_seconds gauge"
                                );
                                metric!(
                                    "pipistrelle_publish_route_latency_p50_seconds {:.6}",
                                    state.publish_route_latency.quantile_seconds(0.50)
                                );
                                metric!(
                                    "# HELP pipistrelle_publish_route_latency_p95_seconds Approximate P95 publish routing latency"
                                );
                                metric!(
                                    "# TYPE pipistrelle_publish_route_latency_p95_seconds gauge"
                                );
                                metric!(
                                    "pipistrelle_publish_route_latency_p95_seconds {:.6}",
                                    state.publish_route_latency.quantile_seconds(0.95)
                                );
                                metric!(
                                    "# HELP pipistrelle_publish_route_latency_p99_seconds Approximate P99 publish routing latency"
                                );
                                metric!(
                                    "# TYPE pipistrelle_publish_route_latency_p99_seconds gauge"
                                );
                                metric!(
                                    "pipistrelle_publish_route_latency_p99_seconds {:.6}",
                                    state.publish_route_latency.quantile_seconds(0.99)
                                );

                                metric!(
                                    "# HELP pipistrelle_build_info Pipistrelle build information"
                                );
                                metric!("# TYPE pipistrelle_build_info gauge");
                                metric!(
                                    "pipistrelle_build_info{{version=\"{}\",series=\"{}\",tls_profile=\"{}\"}} 1",
                                    version::VERSION,
                                    version::SERIES,
                                    tls_profile.as_str()
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
                                    "{{\"name\":\"pipistrelle\",\"version\":\"{}\",\"series\":\"{}\",\"mqtt\":\"5.0\",\"tls\":\"1.3\",\"tls_profile\":\"{}\",\"pqc_kx\":\"X25519MLKEM768\",\"client_queue_capacity\":{},\"max_subscriptions_per_client\":{},\"slow_consumer_policy\":\"{}\",\"slow_consumer_timeout_ms\":{},\"bridge_queue_capacity\":{},\"bridge_queue_policy\":\"{}\",\"latency_sample_rate\":{}}}\n",
                                    version::VERSION,
                                    version::SERIES,
                                    tls_profile.as_str(),
                                    state.client_queue_capacity,
                                    state.max_subscriptions_per_client,
                                    state.slow_consumer_policy.as_str(),
                                    state.slow_consumer_timeout.as_millis(),
                                    state.bridge_queue_capacity,
                                    state.bridge_queue_policy.as_str(),
                                    state.publish_route_latency_sample_rate,
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
