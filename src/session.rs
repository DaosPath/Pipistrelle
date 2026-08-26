use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc};
use tracing::{debug, error, info, warn};

use crate::codec::{Packet, Publish, PublishProperties, encode_packet};
use crate::latency::LatencyHistogram;
use crate::router::TopicRouter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlowConsumerPolicy {
    Backpressure,
    Disconnect,
}

impl SlowConsumerPolicy {
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "disconnect" => Self::Disconnect,
            _ => Self::Backpressure,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Backpressure => "backpressure",
            Self::Disconnect => "disconnect",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeQueuePolicy {
    DropNewest,
    Backpressure,
}

impl BridgeQueuePolicy {
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "backpressure" => Self::Backpressure,
            _ => Self::DropNewest,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::DropNewest => "drop-newest",
            Self::Backpressure => "backpressure",
        }
    }
}

#[derive(Clone)]
pub struct BridgeQueueHandle {
    pub sender: mpsc::Sender<(String, Vec<u8>)>,
    pub topic_prefix: Arc<str>,
}

/// Information about a message currently in-flight (QoS > 0)
#[derive(Debug, Clone)]
pub struct InFlightMessage {
    pub packet_id: u16,
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub sent_at: Instant,
}

/// Represents an active or offline client session
pub struct ClientSession {
    pub client_id: String,
    pub username: Option<String>,
    pub clean_start: bool,
    pub keep_alive: u16,
    pub session_expiry_interval: u32,
    pub last_activity: RwLock<Instant>,

    // Channel to send raw serialized bytes to the client's TCP writer task
    pub sender: mpsc::Sender<Vec<u8>>,

    // Topic aliases sent by the client: Alias ID -> Topic String
    pub topic_aliases: RwLock<HashMap<u16, String>>,

    // Subscription/quota state
    pub subscriptions: RwLock<HashSet<String>>,

    // QoS 1/2 state
    next_packet_id: AtomicU16,
    pub in_flight: RwLock<HashMap<u16, InFlightMessage>>,

    // Slow-consumer cancellation path
    disconnect_requested: AtomicBool,
    pub disconnect_notify: Notify,
}

impl ClientSession {
    pub fn new(
        client_id: String,
        username: Option<String>,
        clean_start: bool,
        keep_alive: u16,
        session_expiry_interval: u32,
        sender: mpsc::Sender<Vec<u8>>,
    ) -> Self {
        Self {
            client_id,
            username,
            clean_start,
            keep_alive,
            session_expiry_interval,
            last_activity: RwLock::new(Instant::now()),
            sender,
            topic_aliases: RwLock::new(HashMap::new()),
            subscriptions: RwLock::new(HashSet::new()),
            next_packet_id: AtomicU16::new(1),
            in_flight: RwLock::new(HashMap::new()),
            disconnect_requested: AtomicBool::new(false),
            disconnect_notify: Notify::new(),
        }
    }

    pub fn update_activity(&self) {
        *self.last_activity.write() = Instant::now();
    }

    pub fn get_next_packet_id(&self) -> u16 {
        // Increment and handle wrap-around (1 to 65535, 0 is reserved)
        let mut id = self.next_packet_id.fetch_add(1, Ordering::SeqCst);
        if id == 0 {
            id = self.next_packet_id.fetch_add(1, Ordering::SeqCst);
        }
        id
    }

    pub fn add_in_flight(&self, packet_id: u16, topic: &str, payload: &[u8], qos: u8) {
        let msg = InFlightMessage {
            packet_id,
            topic: topic.to_string(),
            payload: payload.to_vec(),
            qos,
            sent_at: Instant::now(),
        };
        self.in_flight.write().insert(packet_id, msg);
    }

    pub fn remove_in_flight(&self, packet_id: u16) -> Option<InFlightMessage> {
        self.in_flight.write().remove(&packet_id)
    }

    pub fn request_disconnect(&self) -> bool {
        if self
            .disconnect_requested
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.disconnect_notify.notify_waiters();
            true
        } else {
            false
        }
    }

    pub fn disconnect_requested(&self) -> bool {
        self.disconnect_requested.load(Ordering::Acquire)
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.read().len()
    }
}

/// Global shared state of the Pipistrelle broker
pub struct BrokerState {
    pub router: TopicRouter,
    // Client ID -> Active session reference
    pub sessions: RwLock<HashMap<String, Arc<ClientSession>>>,
    // Auth and ACL engine
    pub auth: crate::config::AuthConfig,
    // Database Persistence Engine
    pub db: Arc<crate::persistence::Persistence>,
    // Round-robin counters for shared subscription groups
    shared_group_counters: RwLock<HashMap<String, AtomicUsize>>,
    // Prometheus Metrics
    pub metrics_messages_published: AtomicUsize,
    pub metrics_subscriptions: AtomicUsize,
    pub metrics_tls_pqc_handshakes: AtomicUsize,
    pub metrics_tls_classical_handshakes: AtomicUsize,
    pub metrics_client_queue_backpressure_events: AtomicUsize,
    pub metrics_client_queue_backpressure_wait_ns: AtomicU64,
    pub metrics_slow_consumer_disconnects: AtomicUsize,
    pub metrics_subscription_quota_rejections: AtomicUsize,
    pub metrics_bridge_queue_backpressure_events: AtomicUsize,
    pub metrics_bridge_queue_backpressure_wait_ns: AtomicU64,
    pub metrics_bridge_queue_dropped: AtomicUsize,
    pub publish_route_latency: LatencyHistogram,
    pub publish_route_latency_sample_rate: usize,

    // Runtime limits/policies
    pub client_queue_capacity: usize,
    pub max_subscriptions_per_client: usize,
    pub slow_consumer_policy: SlowConsumerPolicy,
    pub slow_consumer_timeout: Duration,
    pub bridge_queue_capacity: usize,
    pub bridge_queue_policy: BridgeQueuePolicy,

    // Bridge channel
    pub bridge_sender: RwLock<Option<BridgeQueueHandle>>,
}

impl BrokerState {
    pub fn new() -> Self {
        let client_queue_capacity = std::env::var("PIPISTRELLE_CLIENT_QUEUE_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|value| value.clamp(16, 65_536))
            .unwrap_or(1_024);
        let max_subscriptions_per_client =
            std::env::var("PIPISTRELLE_MAX_SUBSCRIPTIONS_PER_CLIENT")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .map(|value| value.clamp(1, 65_535))
                .unwrap_or(256);
        let slow_consumer_policy = SlowConsumerPolicy::parse(
            &std::env::var("PIPISTRELLE_SLOW_CONSUMER_POLICY")
                .unwrap_or_else(|_| "backpressure".to_string()),
        );
        let slow_consumer_timeout = Duration::from_millis(
            std::env::var("PIPISTRELLE_SLOW_CONSUMER_TIMEOUT_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map(|value| value.clamp(10, 60_000))
                .unwrap_or(5_000),
        );
        let bridge_queue_capacity = std::env::var("PIPISTRELLE_BRIDGE_QUEUE_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|value| value.clamp(16, 262_144))
            .unwrap_or(4_096);
        let bridge_queue_policy = BridgeQueuePolicy::parse(
            &std::env::var("PIPISTRELLE_BRIDGE_QUEUE_POLICY")
                .unwrap_or_else(|_| "drop-newest".to_string()),
        );
        let publish_route_latency_sample_rate = std::env::var("PIPISTRELLE_LATENCY_SAMPLE_RATE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|value| value.clamp(1, 65_536).next_power_of_two().min(65_536))
            .unwrap_or(64);
        info!(
            "Client limits: outbound_queue={}, subscriptions={}, slow_consumer={} ({} ms)",
            client_queue_capacity,
            max_subscriptions_per_client,
            slow_consumer_policy.as_str(),
            slow_consumer_timeout.as_millis(),
        );
        info!(
            "Bridge queue: capacity={}, policy={}; latency sample rate=1/{}",
            bridge_queue_capacity,
            bridge_queue_policy.as_str(),
            publish_route_latency_sample_rate,
        );

        Self {
            router: TopicRouter::new(),
            sessions: RwLock::new(HashMap::new()),
            auth: crate::config::AuthConfig::load(),
            db: Arc::new(crate::persistence::Persistence::new()),
            shared_group_counters: RwLock::new(HashMap::new()),
            metrics_messages_published: AtomicUsize::new(0),
            metrics_subscriptions: AtomicUsize::new(0),
            metrics_tls_pqc_handshakes: AtomicUsize::new(0),
            metrics_tls_classical_handshakes: AtomicUsize::new(0),
            metrics_client_queue_backpressure_events: AtomicUsize::new(0),
            metrics_client_queue_backpressure_wait_ns: AtomicU64::new(0),
            metrics_slow_consumer_disconnects: AtomicUsize::new(0),
            metrics_subscription_quota_rejections: AtomicUsize::new(0),
            metrics_bridge_queue_backpressure_events: AtomicUsize::new(0),
            metrics_bridge_queue_backpressure_wait_ns: AtomicU64::new(0),
            metrics_bridge_queue_dropped: AtomicUsize::new(0),
            publish_route_latency: LatencyHistogram::new(),
            publish_route_latency_sample_rate,
            client_queue_capacity,
            max_subscriptions_per_client,
            slow_consumer_policy,
            slow_consumer_timeout,
            bridge_queue_capacity,
            bridge_queue_policy,
            bridge_sender: RwLock::new(None),
        }
    }

    /// Restores all sessions, subscriptions, and in-flight messages from SQLite DB on boot.
    pub async fn restore_sessions_from_db(&self) {
        info!("Restoring persistent state from database...");

        // 1. Restore sessions
        match self.db.load_all_sessions().await {
            Ok(sessions_loaded) => {
                let mut sessions_guard = self.sessions.write();
                for (client_id, username, clean_start, expiry) in sessions_loaded {
                    // Create offline session with dummy sender channel (replaced when client reconnects)
                    let (tx, _) = mpsc::channel::<Vec<u8>>(self.client_queue_capacity);
                    let session = Arc::new(ClientSession::new(
                        client_id.clone(),
                        username,
                        clean_start,
                        0, // keep-alive is 0 while offline
                        expiry,
                        tx,
                    ));
                    sessions_guard.insert(client_id, session);
                }
                info!("Restored {} session(s) from database", sessions_guard.len());
            }
            Err(e) => {
                error!("Failed to load sessions from database: {:?}", e);
            }
        }

        // 2. Restore subscriptions
        match self.db.load_all_subscriptions().await {
            Ok(subs_loaded) => {
                for (client_id, topic_filter, qos, sub_id) in subs_loaded {
                    if let Some(session) = self.sessions.read().get(&client_id).cloned() {
                        session.subscriptions.write().insert(topic_filter.clone());
                    }
                    self.router
                        .subscribe(&client_id, &topic_filter, qos, sub_id);
                }
                info!("Restored subscriptions from database");
            }
            Err(e) => {
                error!("Failed to load subscriptions from database: {:?}", e);
            }
        }

        // 3. Restore in-flight messages
        match self.db.load_all_in_flight().await {
            Ok(inflight_loaded) => {
                let sessions_guard = self.sessions.read();
                let mut count = 0;
                for (client_id, packet_id, topic, payload, qos) in inflight_loaded {
                    if let Some(session) = sessions_guard.get(&client_id) {
                        session.add_in_flight(packet_id, &topic, &payload, qos);
                        count += 1;
                    }
                }
                info!("Restored {} in-flight message(s) from database", count);
            }
            Err(e) => {
                error!("Failed to load in-flight messages from database: {:?}", e);
            }
        }
    }

    /// Registers a new client session, replacing any existing active session for the same client ID.
    pub fn register_session(&self, session: Arc<ClientSession>) {
        let mut sessions = self.sessions.write();

        let client_id = session.client_id.clone();
        let username = session.username.clone();
        let clean_start = session.clean_start;
        let expiry = session.session_expiry_interval;

        if let Some(old) = sessions.insert(client_id.clone(), session.clone()) {
            info!("Replacing existing session for client: {}", old.client_id);
        }

        // Persist session if it's persistent (session expiry > 0)
        if expiry > 0 {
            let db = self.db.clone();
            tokio::spawn(async move {
                db.save_session(client_id, username, clean_start, expiry)
                    .await;
            });
        }
    }

    /// Removes a client session (e.g. on clean disconnect or session expiration)
    pub fn remove_session(&self, client_id: &str) {
        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.remove(client_id) {
            info!("Removed session for client: {}", client_id);

            // A clean/non-persistent session must leave no routing state behind.
            if session.clean_start || session.session_expiry_interval == 0 {
                let removed = self.router.remove_client(client_id);
                if removed > 0 {
                    debug!(
                        "Removed {} subscription(s) for disconnected client {}",
                        removed, client_id
                    );
                }
                let db = self.db.clone();
                let cid = client_id.to_string();
                tokio::spawn(async move {
                    db.delete_session(cid).await;
                });
            }
        }
    }

    /// Processes a subscription request and enforces the per-client quota.
    /// Returns false when the client has exhausted its subscription allowance.
    pub fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &str,
        qos: u8,
        subscription_identifier: Option<u32>,
    ) -> bool {
        if let Some(session) = self.sessions.read().get(client_id).cloned() {
            let mut subscriptions = session.subscriptions.write();
            let is_new = !subscriptions.contains(topic_filter);
            if is_new && subscriptions.len() >= self.max_subscriptions_per_client {
                self.metrics_subscription_quota_rejections
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Client {} exceeded subscription quota ({})",
                    client_id, self.max_subscriptions_per_client
                );
                return false;
            }
            subscriptions.insert(topic_filter.to_string());
        }

        self.metrics_subscriptions.fetch_add(1, Ordering::Relaxed);
        self.router
            .subscribe(client_id, topic_filter, qos, subscription_identifier);
        debug!(
            "Client {} subscribed to {} with QoS {}",
            client_id, topic_filter, qos
        );

        let db = self.db.clone();
        let cid = client_id.to_string();
        let filter = topic_filter.to_string();
        tokio::spawn(async move {
            db.save_subscription(cid, filter, qos, subscription_identifier)
                .await;
        });
        true
    }

    /// Processes unsubscription request
    pub fn unsubscribe(&self, client_id: &str, topic_filter: &str) -> bool {
        let removed = self.router.unsubscribe(client_id, topic_filter);
        if removed {
            if let Some(session) = self.sessions.read().get(client_id).cloned() {
                session.subscriptions.write().remove(topic_filter);
            }
            debug!("Client {} unsubscribed from {}", client_id, topic_filter);

            // Delete persistent subscription
            let db = self.db.clone();
            let cid = client_id.to_string();
            let filter = topic_filter.to_string();
            tokio::spawn(async move {
                db.delete_subscription(cid, filter).await;
            });
        }
        removed
    }

    pub async fn route_publish(
        &self,
        _from_client: &str,
        topic: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
    ) {
        let publish_sequence = self
            .metrics_messages_published
            .fetch_add(1, Ordering::Relaxed);
        let sample_latency = publish_sequence & (self.publish_route_latency_sample_rate - 1) == 0;
        let route_started = sample_latency.then(Instant::now);

        // The remote bridge is isolated behind its own bounded queue. Only topics
        // matching the bridge prefix are queued, so unrelated traffic never pays
        // for a slow or disconnected remote broker.
        if _from_client != "bridge_client" {
            let bridge = self.bridge_sender.read().clone();
            if let Some(bridge) = bridge {
                if topic.starts_with(bridge.topic_prefix.as_ref()) {
                    let message = (topic.to_string(), payload.to_vec());
                    match bridge.sender.try_send(message) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(message)) => {
                            self.metrics_bridge_queue_backpressure_events
                                .fetch_add(1, Ordering::Relaxed);
                            match self.bridge_queue_policy {
                                BridgeQueuePolicy::DropNewest => {
                                    self.metrics_bridge_queue_dropped
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                BridgeQueuePolicy::Backpressure => {
                                    let started = Instant::now();
                                    let _ = bridge.sender.send(message).await;
                                    let waited_ns =
                                        started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                                    self.metrics_bridge_queue_backpressure_wait_ns
                                        .fetch_add(waited_ns, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {}
                    }
                }
            }
        }

        let route = self.router.match_topic(topic);

        for sub in route.normal {
            self.send_publish_to_client(
                &sub.client_id,
                topic,
                payload,
                qos,
                retain,
                sub.subscription_identifier,
            )
            .await;
        }

        for (group, subs) in route.shared {
            if subs.is_empty() {
                continue;
            }

            let selected_sub = {
                let mut counters = self.shared_group_counters.write();
                let counter = counters
                    .entry(group.clone())
                    .or_insert_with(|| AtomicUsize::new(0));
                let index = counter.fetch_add(1, Ordering::Relaxed);
                subs[index % subs.len()].clone()
            };

            debug!(
                "Routing shared publish for group {} to client {}",
                group, selected_sub.client_id
            );
            self.send_publish_to_client(
                &selected_sub.client_id,
                topic,
                payload,
                qos,
                retain,
                selected_sub.subscription_identifier,
            )
            .await;
        }

        if let Some(route_started) = route_started {
            self.publish_route_latency.record(route_started.elapsed());
        }
    }

    /// Sends bytes to a client using a bounded queue. The fast path is non-blocking;
    /// when the queue is full, pressure propagates back to the publisher until the
    /// socket writer drains enough capacity.
    pub async fn send_to_session(&self, session: &ClientSession, bytes: Vec<u8>) -> bool {
        match session.sender.try_send(bytes) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(bytes)) => {
                self.metrics_client_queue_backpressure_events
                    .fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();

                let result = match self.slow_consumer_policy {
                    SlowConsumerPolicy::Backpressure => {
                        session.sender.send(bytes).await.map_err(|_| ())
                    }
                    SlowConsumerPolicy::Disconnect => {
                        match tokio::time::timeout(
                            self.slow_consumer_timeout,
                            session.sender.send(bytes),
                        )
                        .await
                        {
                            Ok(Ok(())) => Ok(()),
                            Ok(Err(_)) => Err(()),
                            Err(_) => {
                                if session.request_disconnect() {
                                    self.metrics_slow_consumer_disconnects
                                        .fetch_add(1, Ordering::Relaxed);
                                    warn!(
                                        "Disconnecting slow consumer {} after {} ms of queue saturation",
                                        session.client_id,
                                        self.slow_consumer_timeout.as_millis(),
                                    );
                                }
                                Err(())
                            }
                        }
                    }
                };

                let waited_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                self.metrics_client_queue_backpressure_wait_ns
                    .fetch_add(waited_ns, Ordering::Relaxed);
                result.is_ok()
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!("Outbound queue closed for client {}", session.client_id);
                false
            }
        }
    }

    /// Serializes and sends a publish message to a specific client session.
    async fn send_publish_to_client(
        &self,
        client_id: &str,
        topic: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
        subscription_identifier: Option<u32>,
    ) {
        let session = { self.sessions.read().get(client_id).cloned() };
        if let Some(session) = session {
            let packet_id = if qos > 0 {
                let pid = session.get_next_packet_id();
                session.add_in_flight(pid, topic, payload, qos);

                // Persist the in-flight QoS 1 message.
                let db = self.db.clone();
                let cid = client_id.to_string();
                let t = topic.to_string();
                let p = payload.to_vec();
                tokio::spawn(async move {
                    db.save_in_flight(cid, pid, t, p, qos).await;
                });

                Some(pid)
            } else {
                None
            };

            let publish_pkt = Packet::Publish(Publish {
                dup: false,
                qos,
                retain,
                topic,
                packet_id,
                properties: PublishProperties {
                    subscription_identifier,
                    ..Default::default()
                },
                payload,
            });

            let mut buf = Vec::new();
            encode_packet(&publish_pkt, &mut buf);
            let _ = self.send_to_session(&session, buf).await;
        }
    }

    /// Gracefully disconnects all active client sessions.
    pub async fn graceful_shutdown(&self) {
        info!("Gracefully disconnecting all clients...");
        let sessions: Vec<Arc<ClientSession>> = self.sessions.read().values().cloned().collect();
        for session in sessions {
            let disconnect_pkt = Packet::Disconnect(crate::codec::Disconnect {
                reason_code: 0x00,
                properties: Default::default(),
            });
            let mut buf = Vec::new();
            encode_packet(&disconnect_pkt, &mut buf);
            let _ = self.send_to_session(&session, buf).await;
        }
        info!("Sent DISCONNECT to all connected clients.");
    }
}

#[cfg(test)]
mod policy_tests {
    use super::*;

    #[test]
    fn policy_parsing_is_stable() {
        assert_eq!(
            SlowConsumerPolicy::parse("disconnect"),
            SlowConsumerPolicy::Disconnect
        );
        assert_eq!(
            SlowConsumerPolicy::parse("anything"),
            SlowConsumerPolicy::Backpressure
        );
        assert_eq!(
            BridgeQueuePolicy::parse("backpressure"),
            BridgeQueuePolicy::Backpressure
        );
        assert_eq!(
            BridgeQueuePolicy::parse("drop-newest"),
            BridgeQueuePolicy::DropNewest
        );
    }

    #[test]
    fn disconnect_request_is_idempotent() {
        let (tx, _rx) = mpsc::channel(1);
        let session = ClientSession::new("slow".to_string(), None, true, 60, 0, tx);
        assert!(session.request_disconnect());
        assert!(!session.request_disconnect());
        assert!(session.disconnect_requested());
    }
}
