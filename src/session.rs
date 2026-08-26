use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc};
use tracing::{debug, error, info, warn};

use crate::codec::{Packet, Publish, PublishProperties, encode_packet, encode_publish_qos0};
use crate::latency::LatencyHistogram;
use crate::router::{TopicRouter, topic_matches_filter};

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

#[derive(Debug, Clone)]
pub struct RetainedMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
}

#[derive(Debug, Clone)]
pub struct IncomingQos2Message {
    pub topic: String,
    pub payload: Vec<u8>,
    pub retain: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutgoingQos2Phase {
    AwaitPubRec = 0,
    AwaitPubComp = 1,
}

impl OutgoingQos2Phase {
    fn from_db(value: u8) -> Self {
        if value == 1 {
            Self::AwaitPubComp
        } else {
            Self::AwaitPubRec
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutgoingQos2Message {
    pub topic: String,
    pub payload: Vec<u8>,
    pub retain: bool,
    pub subscription_identifier: Option<u32>,
    pub phase: OutgoingQos2Phase,
}

#[derive(Debug, Clone)]
pub struct WillMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    pub delay_interval: u32,
}

#[derive(Clone)]
struct PendingWill {
    cancel: Arc<AtomicBool>,
    message: WillMessage,
}

/// Represents an active or offline client session
pub struct ClientSession {
    pub client_id: String,
    pub username: Option<String>,
    pub clean_start: bool,
    pub allow_all_read: bool,
    pub allow_all_write: bool,
    pub keep_alive: u16,
    pub session_expiry_interval: AtomicU32,
    pub last_activity: RwLock<Instant>,
    pub connected: AtomicBool,
    pub will: RwLock<Option<WillMessage>>,

    // Channel to send raw serialized bytes to the client's TCP writer task
    pub sender: mpsc::Sender<Vec<u8>>,

    // Topic aliases sent by the client: Alias ID -> Topic String
    pub topic_aliases: RwLock<HashMap<u16, String>>,

    // Subscription/quota state
    pub subscriptions: RwLock<HashSet<String>>,

    // Per-session publish counter avoids a globally contended cache line.
    pub published_messages: AtomicU64,

    // QoS 1/2 state
    next_packet_id: AtomicU16,
    pub in_flight: RwLock<HashMap<u16, InFlightMessage>>,
    pub incoming_qos2: RwLock<HashMap<u16, IncomingQos2Message>>,
    pub outgoing_qos2: RwLock<HashMap<u16, OutgoingQos2Message>>,

    // Slow-consumer cancellation path
    disconnect_requested: AtomicBool,
    pub disconnect_notify: Notify,
}

impl ClientSession {
    pub fn new(
        client_id: String,
        username: Option<String>,
        clean_start: bool,
        allow_all_read: bool,
        allow_all_write: bool,
        keep_alive: u16,
        session_expiry_interval: u32,
        sender: mpsc::Sender<Vec<u8>>,
    ) -> Self {
        Self {
            client_id,
            username,
            clean_start,
            allow_all_read,
            allow_all_write,
            keep_alive,
            session_expiry_interval: AtomicU32::new(session_expiry_interval),
            last_activity: RwLock::new(Instant::now()),
            connected: AtomicBool::new(true),
            will: RwLock::new(None),
            sender,
            topic_aliases: RwLock::new(HashMap::new()),
            subscriptions: RwLock::new(HashSet::new()),
            published_messages: AtomicU64::new(0),
            next_packet_id: AtomicU16::new(1),
            in_flight: RwLock::new(HashMap::new()),
            incoming_qos2: RwLock::new(HashMap::new()),
            outgoing_qos2: RwLock::new(HashMap::new()),
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

    pub fn session_expiry(&self) -> u32 {
        self.session_expiry_interval.load(Ordering::Acquire)
    }

    pub fn set_session_expiry(&self, value: u32) {
        self.session_expiry_interval.store(value, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscribeOutcome {
    pub accepted: bool,
    pub is_new: bool,
}

/// Global shared state of the Pipistrelle broker
pub struct BrokerState {
    pub router: TopicRouter,
    pub retained: RwLock<HashMap<String, RetainedMessage>>,
    pending_wills: RwLock<HashMap<String, PendingWill>>,
    // Client ID -> Active session reference
    pub sessions: RwLock<HashMap<String, Arc<ClientSession>>>,
    // Auth and ACL engine
    pub auth: crate::config::AuthConfig,
    // Database Persistence Engine
    pub db: Arc<crate::persistence::Persistence>,
    // Round-robin counters for shared subscription groups
    shared_group_counters: RwLock<HashMap<String, AtomicUsize>>,
    // Prometheus Metrics
    pub metrics_messages_published_retired: AtomicU64,
    pub metrics_subscriptions: AtomicUsize,
    pub metrics_tls_pqc_handshakes: AtomicUsize,
    pub metrics_tls_classical_handshakes: AtomicUsize,
    pub metrics_client_queue_backpressure_events: AtomicUsize,
    pub metrics_client_queue_backpressure_wait_ns: AtomicU64,
    pub metrics_slow_consumer_disconnects: AtomicUsize,
    pub metrics_subscription_quota_rejections: AtomicUsize,
    pub metrics_session_takeovers: AtomicUsize,
    pub metrics_wills_published: AtomicUsize,
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
    pub writer_batch_packets: usize,
    pub writer_batch_bytes: usize,

    // Bridge channel
    pub bridge_active: AtomicBool,
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
        let writer_batch_packets = std::env::var("PIPISTRELLE_WRITER_BATCH_PACKETS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|value| value.clamp(1, 4096))
            .unwrap_or(256);
        let writer_batch_bytes = std::env::var("PIPISTRELLE_WRITER_BATCH_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|value| value.clamp(4_096, 4 * 1024 * 1024))
            .unwrap_or(256 * 1024);
        info!(
            "Client limits: outbound_queue={}, subscriptions={}, slow_consumer={} ({} ms)",
            client_queue_capacity,
            max_subscriptions_per_client,
            slow_consumer_policy.as_str(),
            slow_consumer_timeout.as_millis(),
        );
        info!(
            "Bridge queue: capacity={}, policy={}; latency sample rate=1/{}; writer batch={} packets/{} bytes",
            bridge_queue_capacity,
            bridge_queue_policy.as_str(),
            publish_route_latency_sample_rate,
            writer_batch_packets,
            writer_batch_bytes,
        );

        Self {
            router: TopicRouter::new(),
            retained: RwLock::new(HashMap::new()),
            pending_wills: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            auth: crate::config::AuthConfig::load(),
            db: Arc::new(crate::persistence::Persistence::new()),
            shared_group_counters: RwLock::new(HashMap::new()),
            metrics_messages_published_retired: AtomicU64::new(0),
            metrics_subscriptions: AtomicUsize::new(0),
            metrics_tls_pqc_handshakes: AtomicUsize::new(0),
            metrics_tls_classical_handshakes: AtomicUsize::new(0),
            metrics_client_queue_backpressure_events: AtomicUsize::new(0),
            metrics_client_queue_backpressure_wait_ns: AtomicU64::new(0),
            metrics_slow_consumer_disconnects: AtomicUsize::new(0),
            metrics_subscription_quota_rejections: AtomicUsize::new(0),
            metrics_session_takeovers: AtomicUsize::new(0),
            metrics_wills_published: AtomicUsize::new(0),
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
            writer_batch_packets,
            writer_batch_bytes,
            bridge_active: AtomicBool::new(false),
            bridge_sender: RwLock::new(None),
        }
    }

    /// Restores all sessions, subscriptions, and in-flight messages from SQLite DB on boot.
    pub async fn restore_sessions_from_db(self: &Arc<Self>) {
        info!("Restoring persistent state from database...");

        // 1. Restore sessions and reconstruct the remaining Session Expiry timers.
        // A row marked connected means the broker stopped while the network connection
        // was active; the Session Expiry countdown therefore starts at this restart.
        let mut restored_expiry_timers: Vec<(Arc<ClientSession>, Duration)> = Vec::new();
        match self.db.load_all_sessions().await {
            Ok(sessions_loaded) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let mut expired_ids = Vec::new();
                let mut crashed_active_ids = Vec::new();
                {
                    let mut sessions_guard = self.sessions.write();
                    for (client_id, username, clean_start, expiry, last_activity, was_connected) in
                        sessions_loaded
                    {
                        if expiry == 0 {
                            expired_ids.push(client_id);
                            continue;
                        }
                        let remaining = if expiry == u32::MAX {
                            None
                        } else if was_connected {
                            Some(expiry as u64)
                        } else {
                            let elapsed = now.saturating_sub(last_activity);
                            if elapsed >= expiry as u64 {
                                expired_ids.push(client_id);
                                continue;
                            }
                            Some(expiry as u64 - elapsed)
                        };

                        let (tx, _) = mpsc::channel::<Vec<u8>>(self.client_queue_capacity);
                        let auth_username = username.as_deref().unwrap_or("");
                        let allow_all_read = self.auth.authorizes_all(auth_username, "read");
                        let allow_all_write = self.auth.authorizes_all(auth_username, "write");
                        let session = Arc::new(ClientSession::new(
                            client_id.clone(),
                            username,
                            clean_start,
                            allow_all_read,
                            allow_all_write,
                            0,
                            expiry,
                            tx,
                        ));
                        session.connected.store(false, Ordering::Release);
                        if let Some(seconds) = remaining {
                            restored_expiry_timers
                                .push((session.clone(), Duration::from_secs(seconds)));
                        }
                        if was_connected {
                            crashed_active_ids.push(client_id.clone());
                        }
                        sessions_guard.insert(client_id, session);
                    }
                    info!("Restored {} session(s) from database", sessions_guard.len());
                }
                for client_id in crashed_active_ids {
                    self.db.mark_session_offline(client_id).await;
                }
                for client_id in expired_ids {
                    self.db.delete_session(client_id).await;
                }
            }
            Err(e) => {
                error!("Failed to load sessions from database: {:?}", e);
            }
        }

        // 2. Restore subscriptions
        match self.db.load_all_subscriptions().await {
            Ok(subs_loaded) => {
                for (client_id, topic_filter, qos, sub_id, options) in subs_loaded {
                    if let Some(session) = self.sessions.read().get(&client_id).cloned() {
                        session.subscriptions.write().insert(topic_filter.clone());
                    }
                    self.router.subscribe_with_options(
                        &client_id,
                        &topic_filter,
                        qos,
                        sub_id,
                        options & 0x04 != 0,
                        options & 0x08 != 0,
                    );
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

        match self.db.load_retained().await {
            Ok(messages) => {
                let mut retained = self.retained.write();
                for (topic, payload, qos) in messages {
                    retained.insert(
                        topic.clone(),
                        RetainedMessage {
                            topic,
                            payload,
                            qos,
                        },
                    );
                }
                info!(
                    "Restored {} retained message(s) from database",
                    retained.len()
                );
            }
            Err(e) => error!("Failed to load retained messages: {:?}", e),
        }

        match self.db.load_qos2_incoming().await {
            Ok(messages) => {
                let sessions = self.sessions.read();
                for (client_id, packet_id, topic, payload, retain) in messages {
                    if let Some(session) = sessions.get(&client_id) {
                        session.incoming_qos2.write().insert(
                            packet_id,
                            IncomingQos2Message {
                                topic,
                                payload,
                                retain,
                            },
                        );
                    }
                }
            }
            Err(e) => error!("Failed to restore inbound QoS2 state: {:?}", e),
        }

        match self.db.load_qos2_outgoing().await {
            Ok(messages) => {
                let sessions = self.sessions.read();
                for (
                    client_id,
                    packet_id,
                    topic,
                    payload,
                    retain,
                    subscription_identifier,
                    phase,
                ) in messages
                {
                    if let Some(session) = sessions.get(&client_id) {
                        session.outgoing_qos2.write().insert(
                            packet_id,
                            OutgoingQos2Message {
                                topic,
                                payload,
                                retain,
                                subscription_identifier,
                                phase: OutgoingQos2Phase::from_db(phase),
                            },
                        );
                    }
                }
            }
            Err(e) => error!("Failed to restore outbound QoS2 state: {:?}", e),
        }

        for (session, delay) in restored_expiry_timers {
            self.schedule_session_expiry_after(session, delay);
        }
    }

    pub fn existing_session(&self, client_id: &str) -> Option<Arc<ClientSession>> {
        self.sessions.read().get(client_id).cloned()
    }

    pub fn inherit_persistent_state(&self, target: &ClientSession, source: &ClientSession) {
        *target.subscriptions.write() = source.subscriptions.read().clone();
        *target.in_flight.write() = source.in_flight.read().clone();
        *target.incoming_qos2.write() = source.incoming_qos2.read().clone();
        *target.outgoing_qos2.write() = source.outgoing_qos2.read().clone();
    }

    /// Registers the network connection/session object that is current for this ClientID.
    pub async fn register_session(&self, session: Arc<ClientSession>) {
        let client_id = session.client_id.clone();
        let username = session.username.clone();
        let clean_start = session.clean_start;
        let expiry = session.session_expiry_interval.load(Ordering::Acquire);

        {
            let mut sessions = self.sessions.write();
            if let Some(old) = sessions.insert(client_id.clone(), session.clone()) {
                let retired = old.published_messages.load(Ordering::Relaxed);
                self.metrics_messages_published_retired
                    .fetch_add(retired, Ordering::Relaxed);
                info!("Replacing session object for client: {}", old.client_id);
            }
        }

        // Persist the current connection before CONNACK. This avoids a fast
        // disconnect racing an older background save and resurrecting connected=1.
        if expiry > 0 {
            self.db
                .save_session(client_id, username, clean_start, expiry)
                .await;
        }
    }

    /// Discards all session state, used by Clean Start before a replacement is installed.
    pub async fn discard_session_state(&self, client_id: &str) {
        if let Some(old) = self.sessions.write().remove(client_id) {
            let retired = old.published_messages.load(Ordering::Relaxed);
            self.metrics_messages_published_retired
                .fetch_add(retired, Ordering::Relaxed);
        }
        self.router.remove_client(client_id);
        if let Some(pending) = self.pending_wills.write().remove(client_id) {
            pending.cancel.store(true, Ordering::Release);
        }
        self.db.delete_session(client_id.to_string()).await;
    }

    /// Marks a connection offline only if it is still the current object for this ClientID.
    /// This prevents an old taken-over connection from deleting its replacement.
    pub async fn disconnect_session_if_current(&self, session: &Arc<ClientSession>) -> bool {
        let delete_persistent_row = {
            let mut sessions = self.sessions.write();
            let Some(current) = sessions.get(&session.client_id) else {
                return false;
            };
            if !Arc::ptr_eq(current, session) {
                return false;
            }

            session.connected.store(false, Ordering::Release);
            if session.session_expiry_interval.load(Ordering::Acquire) == 0 {
                sessions.remove(&session.client_id);
                let retired = session.published_messages.load(Ordering::Relaxed);
                self.metrics_messages_published_retired
                    .fetch_add(retired, Ordering::Relaxed);
                self.router.remove_client(&session.client_id);
                true
            } else {
                false
            }
        };
        if delete_persistent_row {
            self.db.delete_session(session.client_id.clone()).await;
        } else {
            self.db
                .mark_session_offline(session.client_id.clone())
                .await;
        }
        true
    }

    pub fn cancel_pending_will(&self, client_id: &str) -> bool {
        if let Some(pending) = self.pending_wills.write().remove(client_id) {
            pending.cancel.store(true, Ordering::Release);
            true
        } else {
            false
        }
    }

    pub async fn publish_will_now(&self, message: WillMessage) {
        self.metrics_wills_published.fetch_add(1, Ordering::Relaxed);
        if message.retain {
            self.update_retained(&message.topic, &message.payload, message.qos);
        }
        let sequence = self
            .metrics_messages_published_retired
            .fetch_add(1, Ordering::Relaxed);
        self.route_publish(
            "__pipistrelle_will__",
            &message.topic,
            &message.payload,
            message.qos,
            message.retain,
            sequence,
        )
        .await;
    }

    pub async fn publish_pending_will_now(self: &Arc<Self>, client_id: &str) -> bool {
        let pending = self.pending_wills.write().remove(client_id);
        if let Some(pending) = pending {
            pending.cancel.store(true, Ordering::Release);
            self.publish_will_now(pending.message).await;
            true
        } else {
            false
        }
    }

    pub fn schedule_session_expiry(self: &Arc<Self>, session: Arc<ClientSession>) {
        let expiry = session.session_expiry();
        if expiry == 0 || expiry == u32::MAX {
            return;
        }
        self.schedule_session_expiry_after(session, Duration::from_secs(expiry as u64));
    }

    fn schedule_session_expiry_after(
        self: &Arc<Self>,
        session: Arc<ClientSession>,
        delay: Duration,
    ) {
        let state = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let still_current_offline = {
                let sessions = state.sessions.read();
                sessions
                    .get(&session.client_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &session))
                    && !session.connected.load(Ordering::Acquire)
            };
            if !still_current_offline {
                return;
            }
            let _ = state.publish_pending_will_now(&session.client_id).await;
            state.discard_session_state(&session.client_id).await;
        });
    }

    pub fn schedule_will(
        self: &Arc<Self>,
        client_id: String,
        message: WillMessage,
        session_expiry_interval: u32,
    ) {
        let delay = if session_expiry_interval == 0 {
            0
        } else if message.delay_interval == 0 {
            0
        } else {
            message.delay_interval.min(session_expiry_interval)
        };
        let cancel = Arc::new(AtomicBool::new(false));
        if let Some(old) = self.pending_wills.write().insert(
            client_id.clone(),
            PendingWill {
                cancel: cancel.clone(),
                message: message.clone(),
            },
        ) {
            old.cancel.store(true, Ordering::Release);
        }
        let state = self.clone();
        tokio::spawn(async move {
            if delay > 0 {
                tokio::time::sleep(Duration::from_secs(delay as u64)).await;
            }
            if cancel.load(Ordering::Acquire) {
                return;
            }
            let should_publish = {
                let mut pending = state.pending_wills.write();
                match pending.get(&client_id) {
                    Some(current) if Arc::ptr_eq(&current.cancel, &cancel) => {
                        pending.remove(&client_id);
                        true
                    }
                    _ => false,
                }
            };
            if should_publish && !cancel.load(Ordering::Acquire) {
                state.publish_will_now(message).await;
            }
        });
    }

    /// Processes a subscription request and enforces the per-client quota.
    pub fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &str,
        qos: u8,
        subscription_identifier: Option<u32>,
        options: u8,
    ) -> SubscribeOutcome {
        let mut is_new = true;
        if let Some(session) = self.sessions.read().get(client_id).cloned() {
            let mut subscriptions = session.subscriptions.write();
            is_new = !subscriptions.contains(topic_filter);
            if is_new && subscriptions.len() >= self.max_subscriptions_per_client {
                self.metrics_subscription_quota_rejections
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Client {} exceeded subscription quota ({})",
                    client_id, self.max_subscriptions_per_client
                );
                return SubscribeOutcome {
                    accepted: false,
                    is_new,
                };
            }
            subscriptions.insert(topic_filter.to_string());
        }

        self.metrics_subscriptions.fetch_add(1, Ordering::Relaxed);
        self.router.subscribe_with_options(
            client_id,
            topic_filter,
            qos,
            subscription_identifier,
            options & 0x04 != 0,
            options & 0x08 != 0,
        );
        debug!(
            "Client {} subscribed to {} with QoS {} options=0x{:02x}",
            client_id, topic_filter, qos, options
        );

        let db = self.db.clone();
        let cid = client_id.to_string();
        let filter = topic_filter.to_string();
        tokio::spawn(async move {
            db.save_subscription(cid, filter, qos, subscription_identifier, options)
                .await;
        });
        SubscribeOutcome {
            accepted: true,
            is_new,
        }
    }

    pub async fn send_retained_for_subscription(
        &self,
        session: &ClientSession,
        topic_filter: &str,
        granted_qos: u8,
        subscription_identifier: Option<u32>,
    ) {
        let messages: Vec<RetainedMessage> = self
            .retained
            .read()
            .values()
            .filter(|message| topic_matches_filter(&message.topic, topic_filter))
            .cloned()
            .collect();
        for message in messages {
            let qos = message.qos.min(granted_qos);
            self.send_publish_to_client(
                &session.client_id,
                &message.topic,
                &message.payload,
                qos,
                true,
                subscription_identifier,
            )
            .await;
        }
    }

    pub fn update_retained(&self, topic: &str, payload: &[u8], qos: u8) {
        let db = self.db.clone();
        if payload.is_empty() {
            self.retained.write().remove(topic);
            let topic = topic.to_string();
            tokio::spawn(async move { db.delete_retained(topic).await });
        } else {
            self.retained.write().insert(
                topic.to_string(),
                RetainedMessage {
                    topic: topic.to_string(),
                    payload: payload.to_vec(),
                    qos,
                },
            );
            let topic = topic.to_string();
            let payload = payload.to_vec();
            tokio::spawn(async move { db.save_retained(topic, payload, qos).await });
        }
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
        from_client: &str,
        topic: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
        publish_sequence: u64,
    ) {
        let sample_latency =
            publish_sequence & (self.publish_route_latency_sample_rate as u64 - 1) == 0;
        let route_started = sample_latency.then(Instant::now);

        // The dominant ingest case has no matching subscribers and no active bridge.
        // Avoid router/bridge locks and all routing allocations in that case.
        if !self.router.has_routes() && !self.bridge_active.load(Ordering::Relaxed) {
            if let Some(route_started) = route_started {
                self.publish_route_latency.record(route_started.elapsed());
            }
            return;
        }

        // The remote bridge is isolated behind its own bounded queue. Only topics
        // matching the bridge prefix are queued, so unrelated traffic never pays
        // for a slow or disconnected remote broker.
        if from_client != "bridge_client" {
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

        // Exact-only routing is the common telemetry/service case. Subscription
        // mutations rebuild an Arc slice; publishes only clone the Arc and iterate.
        if self.router.has_only_exact_routes() {
            if let Some(subscriptions) = self.router.match_exact(topic) {
                for sub in subscriptions.iter() {
                    if sub.no_local && sub.client_id == from_client {
                        continue;
                    }
                    self.send_publish_to_client(
                        &sub.client_id,
                        topic,
                        payload,
                        qos.min(sub.qos),
                        retain && sub.retain_as_published,
                        sub.subscription_identifier,
                    )
                    .await;
                }
            }
            if let Some(route_started) = route_started {
                self.publish_route_latency.record(route_started.elapsed());
            }
            return;
        }

        let route = self.router.match_topic(topic);

        for sub in route.normal {
            if sub.no_local && sub.client_id == from_client {
                continue;
            }
            self.send_publish_to_client(
                &sub.client_id,
                topic,
                payload,
                qos.min(sub.qos),
                retain && sub.retain_as_published,
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
                qos.min(selected_sub.qos),
                retain && selected_sub.retain_as_published,
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
                if qos == 1 {
                    session.add_in_flight(pid, topic, payload, qos);
                    let db = self.db.clone();
                    let cid = client_id.to_string();
                    let t = topic.to_string();
                    let p = payload.to_vec();
                    tokio::spawn(async move {
                        db.save_in_flight(cid, pid, t, p, qos).await;
                    });
                } else {
                    let message = OutgoingQos2Message {
                        topic: topic.to_string(),
                        payload: payload.to_vec(),
                        retain,
                        subscription_identifier,
                        phase: OutgoingQos2Phase::AwaitPubRec,
                    };
                    session.outgoing_qos2.write().insert(pid, message.clone());
                    if session.session_expiry() > 0 {
                        self.db
                            .save_qos2_outgoing(
                                client_id.to_string(),
                                pid,
                                message.topic,
                                message.payload,
                                message.retain,
                                message.subscription_identifier,
                                message.phase as u8,
                            )
                            .await;
                    }
                }
                Some(pid)
            } else {
                None
            };

            let buf = if qos == 0 {
                encode_publish_qos0(topic, payload, retain, subscription_identifier)
            } else {
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
                buf
            };
            let _ = self.send_to_session(&session, buf).await;
        }
    }

    pub async fn resume_persistent_outgoing(&self, session: &ClientSession) {
        let qos1: Vec<InFlightMessage> = session.in_flight.read().values().cloned().collect();
        for message in qos1 {
            let publish = Packet::Publish(Publish {
                dup: true,
                qos: 1,
                retain: false,
                topic: &message.topic,
                packet_id: Some(message.packet_id),
                properties: PublishProperties::default(),
                payload: &message.payload,
            });
            let mut buf = Vec::new();
            encode_packet(&publish, &mut buf);
            let _ = self.send_to_session(session, buf).await;
        }

        let qos2: Vec<(u16, OutgoingQos2Message)> = session
            .outgoing_qos2
            .read()
            .iter()
            .map(|(packet_id, message)| (*packet_id, message.clone()))
            .collect();
        for (packet_id, message) in qos2 {
            let packet = match message.phase {
                OutgoingQos2Phase::AwaitPubRec => Packet::Publish(Publish {
                    dup: true,
                    qos: 2,
                    retain: message.retain,
                    topic: &message.topic,
                    packet_id: Some(packet_id),
                    properties: PublishProperties {
                        subscription_identifier: message.subscription_identifier,
                        ..Default::default()
                    },
                    payload: &message.payload,
                }),
                OutgoingQos2Phase::AwaitPubComp => Packet::PubRel(crate::codec::PubAck {
                    packet_id,
                    reason_code: 0x00,
                    properties: Default::default(),
                }),
            };
            let mut buf = Vec::new();
            encode_packet(&packet, &mut buf);
            let _ = self.send_to_session(session, buf).await;
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
        let session = ClientSession::new("slow".to_string(), None, true, false, false, 60, 0, tx);
        assert!(session.request_disconnect());
        assert!(!session.request_disconnect());
        assert!(session.disconnect_requested());
    }
}
