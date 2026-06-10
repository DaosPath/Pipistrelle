use std::sync::Arc;
use parking_lot::Mutex;
use rusqlite::Connection;
use tracing::{info, error};

pub struct Persistence {
    conn: Arc<Mutex<Connection>>,
}

impl Persistence {
    pub fn new() -> Self {
        let conn = Connection::open("pipistrelle.db").expect("Failed to open database pipistrelle.db");
        
        // Configure WAL (Write-Ahead Logging) mode and synchronous normal to resist sudden power losses on ARM
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;"
        ).expect("Failed to configure database performance parameters");

        // Initialize schema
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions (
                client_id TEXT PRIMARY KEY,
                username TEXT,
                clean_start INTEGER,
                session_expiry_interval INTEGER,
                last_activity INTEGER
            )",
            [],
        ).expect("Failed to create sessions table");

        conn.execute(
            "CREATE TABLE IF NOT EXISTS subscriptions (
                client_id TEXT,
                topic_filter TEXT,
                qos INTEGER,
                sub_id INTEGER,
                PRIMARY KEY (client_id, topic_filter)
            )",
            [],
        ).expect("Failed to create subscriptions table");

        conn.execute(
            "CREATE TABLE IF NOT EXISTS in_flight (
                client_id TEXT,
                packet_id INTEGER,
                topic TEXT,
                payload BLOB,
                qos INTEGER,
                PRIMARY KEY (client_id, packet_id)
            )",
            [],
        ).expect("Failed to create in_flight table");

        info!("SQLite persistence engine initialized (pipistrelle.db)");

        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    pub async fn save_session(&self, client_id: String, username: Option<String>, clean_start: bool, session_expiry_interval: u32) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let clean_start_int = if clean_start { 1 } else { 0 };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
                
            if let Err(e) = conn.execute(
                "INSERT OR REPLACE INTO sessions (client_id, username, clean_start, session_expiry_interval, last_activity)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    &client_id,
                    username,
                    clean_start_int,
                    session_expiry_interval,
                    now,
                ),
            ) {
                error!("Failed to save session for {}: {:?}", client_id, e);
            }
        }).await.unwrap();
    }

    pub async fn delete_session(&self, client_id: String) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let _ = conn.execute("DELETE FROM sessions WHERE client_id = ?1", [&client_id]);
            let _ = conn.execute("DELETE FROM subscriptions WHERE client_id = ?1", [&client_id]);
            let _ = conn.execute("DELETE FROM in_flight WHERE client_id = ?1", [&client_id]);
        }).await.unwrap();
    }

    pub async fn save_subscription(&self, client_id: String, topic_filter: String, qos: u8, sub_id: Option<u32>) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            if let Err(e) = conn.execute(
                "INSERT OR REPLACE INTO subscriptions (client_id, topic_filter, qos, sub_id)
                 VALUES (?1, ?2, ?3, ?4)",
                (
                    &client_id,
                    &topic_filter,
                    qos,
                    sub_id,
                ),
            ) {
                error!("Failed to save subscription for {} on {}: {:?}", client_id, topic_filter, e);
            }
        }).await.unwrap();
    }

    pub async fn delete_subscription(&self, client_id: String, topic_filter: String) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let _ = conn.execute(
                "DELETE FROM subscriptions WHERE client_id = ?1 AND topic_filter = ?2",
                (&client_id, &topic_filter),
            );
        }).await.unwrap();
    }

    pub async fn save_in_flight(&self, client_id: String, packet_id: u16, topic: String, payload: Vec<u8>, qos: u8) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            if let Err(e) = conn.execute(
                "INSERT OR REPLACE INTO in_flight (client_id, packet_id, topic, payload, qos)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    &client_id,
                    packet_id,
                    &topic,
                    &payload,
                    qos,
                ),
            ) {
                error!("Failed to save in-flight message for {} with ID {}: {:?}", client_id, packet_id, e);
            }
        }).await.unwrap();
    }

    pub async fn delete_in_flight(&self, client_id: String, packet_id: u16) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let _ = conn.execute(
                "DELETE FROM in_flight WHERE client_id = ?1 AND packet_id = ?2",
                (&client_id, packet_id),
            );
        }).await.unwrap();
    }

    pub async fn load_all_sessions(&self) -> Result<Vec<(String, Option<String>, bool, u32)>, rusqlite::Error> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let mut stmt = conn.prepare("SELECT client_id, username, clean_start, session_expiry_interval FROM sessions")?;
            let rows = stmt.query_map([], |row| {
                let client_id: String = row.get(0)?;
                let username: Option<String> = row.get(1)?;
                let clean_start_int: i32 = row.get(2)?;
                let expiry: u32 = row.get(3)?;
                Ok((client_id, username, clean_start_int != 0, expiry))
            })?;
            let mut result = Vec::new();
            for r in rows {
                result.push(r?);
            }
            Ok(result)
        }).await.unwrap()
    }

    pub async fn load_all_subscriptions(&self) -> Result<Vec<(String, String, u8, Option<u32>)>, rusqlite::Error> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let mut stmt = conn.prepare("SELECT client_id, topic_filter, qos, sub_id FROM subscriptions")?;
            let rows = stmt.query_map([], |row| {
                let client_id: String = row.get(0)?;
                let topic_filter: String = row.get(1)?;
                let qos: u8 = row.get(2)?;
                let sub_id: Option<u32> = row.get(3)?;
                Ok((client_id, topic_filter, qos, sub_id))
            })?;
            let mut result = Vec::new();
            for r in rows {
                result.push(r?);
            }
            Ok(result)
        }).await.unwrap()
    }

    pub async fn load_all_in_flight(&self) -> Result<Vec<(String, u16, String, Vec<u8>, u8)>, rusqlite::Error> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let mut stmt = conn.prepare("SELECT client_id, packet_id, topic, payload, qos FROM in_flight")?;
            let rows = stmt.query_map([], |row| {
                let client_id: String = row.get(0)?;
                let packet_id: u16 = row.get(1)?;
                let topic: String = row.get(2)?;
                let payload: Vec<u8> = row.get(3)?;
                let qos: u8 = row.get(4)?;
                Ok((client_id, packet_id, topic, payload, qos))
            })?;
            let mut result = Vec::new();
            for r in rows {
                result.push(r?);
            }
            Ok(result)
        }).await.unwrap()
    }
}
