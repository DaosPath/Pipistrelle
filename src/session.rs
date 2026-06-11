use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::time::Instant;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{info, warn, debug, error};

use crate::router::TopicRouter;
use crate::codec::{Packet, Publish, PublishProperties, encode_packet};

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
    pub sender: mpsc::UnboundedSender<Vec<u8>>,
    
    // Topic aliases sent by the client: Alias ID -> Topic String
    pub topic_aliases: RwLock<HashMap<u16, String>>,
    
    // QoS 1/2 state
    next_packet_id: AtomicU16,
    pub in_flight: RwLock<HashMap<u16, InFlightMessage>>,
}

impl ClientSession {
    pub fn new(
        client_id: String,
        username: Option<String>,
        clean_start: bool,
        keep_alive: u16,
        session_expiry_interval: u32,
        sender: mpsc::UnboundedSender<Vec<u8>>,
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
            next_packet_id: AtomicU16::new(1),
            in_flight: RwLock::new(HashMap::new()),
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
    // Bridge channel
    pub bridge_sender: RwLock<Option<mpsc::UnboundedSender<(String, Vec<u8>)>>>,
}

impl BrokerState {
    pub fn new() -> Self {
        Self {
            router: TopicRouter::new(),
            sessions: RwLock::new(HashMap::new()),
            auth: crate::config::AuthConfig::load(),
            db: Arc::new(crate::persistence::Persistence::new()),
            shared_group_counters: RwLock::new(HashMap::new()),
            metrics_messages_published: AtomicUsize::new(0),
            metrics_subscriptions: AtomicUsize::new(0),
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
                    let (tx, _) = mpsc::unbounded_channel::<Vec<u8>>();
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
                    self.router.subscribe(&client_id, &topic_filter, qos, sub_id);
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
                db.save_session(client_id, username, clean_start, expiry).await;
            });
        }
    }

    /// Removes a client session (e.g. on clean disconnect or session expiration)
    pub fn remove_session(&self, client_id: &str) {
        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.remove(client_id) {
            info!("Removed session for client: {}", client_id);
            
            // Delete persistent session from DB if it was clean start or session expired
            if session.clean_start || session.session_expiry_interval == 0 {
                let db = self.db.clone();
                let cid = client_id.to_string();
                tokio::spawn(async move {
                    db.delete_session(cid).await;
                });
            }
        }
    }

    /// Processes subscription request and registers it in the router
    pub fn subscribe(&self, client_id: &str, topic_filter: &str, qos: u8, subscription_identifier: Option<u32>) {
        self.metrics_subscriptions.fetch_add(1, Ordering::Relaxed);
        self.router.subscribe(client_id, topic_filter, qos, subscription_identifier);
        debug!("Client {} subscribed to {} with QoS {}", client_id, topic_filter, qos);

        // Persist subscription
        let db = self.db.clone();
        let cid = client_id.to_string();
        let filter = topic_filter.to_string();
        tokio::spawn(async move {
            db.save_subscription(cid, filter, qos, subscription_identifier).await;
        });
    }

    /// Processes unsubscription request
    pub fn unsubscribe(&self, client_id: &str, topic_filter: &str) -> bool {
        let removed = self.router.unsubscribe(client_id, topic_filter);
        if removed {
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

    pub fn route_publish(&self, _from_client: &str, topic: &str, payload: &[u8], qos: u8, retain: bool) {
        self.metrics_messages_published.fetch_add(1, Ordering::Relaxed);
        
        // Forward local publish to the bridge if it didn't originate from the bridge itself
        if _from_client != "bridge_client" {
            if let Some(tx) = &*self.bridge_sender.read() {
                let _ = tx.send((topic.to_string(), payload.to_vec()));
            }
        }

        let route = self.router.match_topic(topic);

        // 1. Deliver to normal subscribers
        for sub in route.normal {
            self.send_publish_to_client(&sub.client_id, topic, payload, qos, retain, sub.subscription_identifier);
        }

        // 2. Deliver to shared subscribers (balance load per group using round-robin)
        for (group, subs) in route.shared {
            if subs.is_empty() {
                continue;
            }

            let mut counters = self.shared_group_counters.write();
            let counter = counters
                .entry(group.clone())
                .or_insert_with(|| AtomicUsize::new(0));

            let index = counter.fetch_add(1, Ordering::Relaxed);
            let selected_sub = &subs[index % subs.len()];
            
            debug!("Routing shared publish for group {} to client {}", group, selected_sub.client_id);
            self.send_publish_to_client(
                &selected_sub.client_id,
                topic,
                payload,
                qos,
                retain,
                selected_sub.subscription_identifier,
            );
        }
    }

    /// Serializes and sends a publish message to a specific client session
    fn send_publish_to_client(
        &self,
        client_id: &str,
        topic: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
        subscription_identifier: Option<u32>,
    ) {
        let sessions = self.sessions.read();
        if let Some(session) = sessions.get(client_id) {
            let packet_id = if qos > 0 {
                let pid = session.get_next_packet_id();
                session.add_in_flight(pid, topic, payload, qos);
                
                // Persist the in-flight QoS 1 message
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

            if let Err(e) = session.sender.send(buf) {
                warn!("Failed to send packet to client channel for {}: {}", client_id, e);
            }
        }
    }

    /// Gracefully disconnects all active client sessions.
    pub fn graceful_shutdown(&self) {
        info!("Gracefully disconnecting all clients...");
        let sessions = self.sessions.read();
        for (client_id, session) in sessions.iter() {
            let disconnect_pkt = Packet::Disconnect(crate::codec::Disconnect {
                reason_code: 0x00, // Normal disconnection
                properties: Default::default(),
            });
            let mut buf = Vec::new();
            encode_packet(&disconnect_pkt, &mut buf);
            if let Err(e) = session.sender.send(buf) {
                debug!("Failed to send DISCONNECT to client {}: {}", client_id, e);
            }
        }
        info!("Sent DISCONNECT to all connected clients.");
    }
}

