use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::Arc;
use tracing::{error, info};

pub struct Persistence {
    conn: Arc<Mutex<Connection>>,
}

impl Persistence {
    pub fn new() -> Self {
        let db_path =
            std::env::var("PIPISTRELLE_DB_PATH").unwrap_or_else(|_| "pipistrelle.db".to_string());
        let conn =
            Connection::open(&db_path).expect(&format!("Failed to open database {}", db_path));

        // Configure WAL (Write-Ahead Logging) mode and synchronous normal to resist sudden power losses on ARM
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;",
        )
        .expect("Failed to configure database performance parameters");

        // Initialize schema
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions (
                client_id TEXT PRIMARY KEY,
                username TEXT,
                clean_start INTEGER,
                session_expiry_interval INTEGER,
                last_activity INTEGER,
                connected INTEGER NOT NULL DEFAULT 0
            )",
            [],
        )
        .expect("Failed to create sessions table");
        // Migration for databases created before connection-state tracking.
        let _ = conn.execute(
            "ALTER TABLE sessions ADD COLUMN connected INTEGER NOT NULL DEFAULT 0",
            [],
        );

        conn.execute(
            "CREATE TABLE IF NOT EXISTS subscriptions (
                client_id TEXT,
                topic_filter TEXT,
                qos INTEGER,
                sub_id INTEGER,
                PRIMARY KEY (client_id, topic_filter)
            )",
            [],
        )
        .expect("Failed to create subscriptions table");
        // Migration for databases created before MQTT v5 subscription options were persisted.
        let _ = conn.execute(
            "ALTER TABLE subscriptions ADD COLUMN options INTEGER NOT NULL DEFAULT 0",
            [],
        );

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
        )
        .expect("Failed to create in_flight table");

        conn.execute(
            "CREATE TABLE IF NOT EXISTS retained_messages (
                topic TEXT PRIMARY KEY,
                payload BLOB NOT NULL,
                qos INTEGER NOT NULL
            )",
            [],
        )
        .expect("Failed to create retained_messages table");

        conn.execute(
            "CREATE TABLE IF NOT EXISTS qos2_incoming (
                client_id TEXT NOT NULL,
                packet_id INTEGER NOT NULL,
                topic TEXT NOT NULL,
                payload BLOB NOT NULL,
                retain INTEGER NOT NULL,
                PRIMARY KEY (client_id, packet_id)
            )",
            [],
        )
        .expect("Failed to create qos2_incoming table");

        conn.execute(
            "CREATE TABLE IF NOT EXISTS qos2_outgoing (
                client_id TEXT NOT NULL,
                packet_id INTEGER NOT NULL,
                topic TEXT NOT NULL,
                payload BLOB NOT NULL,
                retain INTEGER NOT NULL,
                subscription_identifier INTEGER,
                phase INTEGER NOT NULL,
                PRIMARY KEY (client_id, packet_id)
            )",
            [],
        )
        .expect("Failed to create qos2_outgoing table");

        info!("SQLite persistence engine initialized ({})", db_path);

        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    pub async fn save_session(
        &self,
        client_id: String,
        username: Option<String>,
        clean_start: bool,
        session_expiry_interval: u32,
    ) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let clean_start_int = if clean_start { 1 } else { 0 };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            if let Err(e) = conn.execute(
                "INSERT OR REPLACE INTO sessions (client_id, username, clean_start, session_expiry_interval, last_activity, connected)
                 VALUES (?1, ?2, ?3, ?4, ?5, 1)",
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

    pub async fn mark_session_offline(&self, client_id: String) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if let Err(e) = conn.execute(
                "UPDATE sessions SET last_activity = ?2, connected = 0 WHERE client_id = ?1",
                (&client_id, now),
            ) {
                error!("Failed to mark session {} offline: {:?}", client_id, e);
            }
        })
        .await
        .unwrap();
    }

    pub async fn delete_session(&self, client_id: String) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let _ = conn.execute("DELETE FROM sessions WHERE client_id = ?1", [&client_id]);
            let _ = conn.execute(
                "DELETE FROM subscriptions WHERE client_id = ?1",
                [&client_id],
            );
            let _ = conn.execute("DELETE FROM in_flight WHERE client_id = ?1", [&client_id]);
            let _ = conn.execute(
                "DELETE FROM qos2_incoming WHERE client_id = ?1",
                [&client_id],
            );
            let _ = conn.execute(
                "DELETE FROM qos2_outgoing WHERE client_id = ?1",
                [&client_id],
            );
        })
        .await
        .unwrap();
    }

    pub async fn save_subscription(
        &self,
        client_id: String,
        topic_filter: String,
        qos: u8,
        sub_id: Option<u32>,
        options: u8,
    ) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            if let Err(e) = conn.execute(
                "INSERT OR REPLACE INTO subscriptions (client_id, topic_filter, qos, sub_id, options)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                (&client_id, &topic_filter, qos, sub_id, options),
            ) {
                error!(
                    "Failed to save subscription for {} on {}: {:?}",
                    client_id, topic_filter, e
                );
            }
        })
        .await
        .unwrap();
    }

    pub async fn delete_subscription(&self, client_id: String, topic_filter: String) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let _ = conn.execute(
                "DELETE FROM subscriptions WHERE client_id = ?1 AND topic_filter = ?2",
                (&client_id, &topic_filter),
            );
        })
        .await
        .unwrap();
    }

    pub async fn save_in_flight(
        &self,
        client_id: String,
        packet_id: u16,
        topic: String,
        payload: Vec<u8>,
        qos: u8,
    ) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            if let Err(e) = conn.execute(
                "INSERT OR REPLACE INTO in_flight (client_id, packet_id, topic, payload, qos)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                (&client_id, packet_id, &topic, &payload, qos),
            ) {
                error!(
                    "Failed to save in-flight message for {} with ID {}: {:?}",
                    client_id, packet_id, e
                );
            }
        })
        .await
        .unwrap();
    }

    pub async fn delete_in_flight(&self, client_id: String, packet_id: u16) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let _ = conn.execute(
                "DELETE FROM in_flight WHERE client_id = ?1 AND packet_id = ?2",
                (&client_id, packet_id),
            );
        })
        .await
        .unwrap();
    }

    pub async fn load_all_sessions(
        &self,
    ) -> Result<Vec<(String, Option<String>, bool, u32, u64, bool)>, rusqlite::Error> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let mut stmt = conn.prepare(
                "SELECT client_id, username, clean_start, session_expiry_interval, last_activity, connected FROM sessions",
            )?;
            let rows = stmt.query_map([], |row| {
                let client_id: String = row.get(0)?;
                let username: Option<String> = row.get(1)?;
                let clean_start_int: i32 = row.get(2)?;
                let expiry: u32 = row.get(3)?;
                let last_activity: u64 = row.get(4)?;
                let connected: i32 = row.get(5)?;
                Ok((
                    client_id,
                    username,
                    clean_start_int != 0,
                    expiry,
                    last_activity,
                    connected != 0,
                ))
            })?;
            let mut result = Vec::new();
            for r in rows {
                result.push(r?);
            }
            Ok(result)
        })
        .await
        .unwrap()
    }

    pub async fn load_all_subscriptions(
        &self,
    ) -> Result<Vec<(String, String, u8, Option<u32>, u8)>, rusqlite::Error> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let mut stmt = conn.prepare(
                "SELECT client_id, topic_filter, qos, sub_id, options FROM subscriptions",
            )?;
            let rows = stmt.query_map([], |row| {
                let client_id: String = row.get(0)?;
                let topic_filter: String = row.get(1)?;
                let qos: u8 = row.get(2)?;
                let sub_id: Option<u32> = row.get(3)?;
                let options: u8 = row.get(4)?;
                Ok((client_id, topic_filter, qos, sub_id, options))
            })?;
            let mut result = Vec::new();
            for r in rows {
                result.push(r?);
            }
            Ok(result)
        })
        .await
        .unwrap()
    }

    pub async fn load_all_in_flight(
        &self,
    ) -> Result<Vec<(String, u16, String, Vec<u8>, u8)>, rusqlite::Error> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let mut stmt =
                conn.prepare("SELECT client_id, packet_id, topic, payload, qos FROM in_flight")?;
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
        })
        .await
        .unwrap()
    }

    pub async fn save_retained(&self, topic: String, payload: Vec<u8>, qos: u8) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            if let Err(e) = conn.execute(
                "INSERT OR REPLACE INTO retained_messages (topic, payload, qos) VALUES (?1, ?2, ?3)",
                (&topic, &payload, qos),
            ) {
                error!("Failed to save retained message for {}: {:?}", topic, e);
            }
        }).await.unwrap();
    }

    pub async fn delete_retained(&self, topic: String) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let _ = conn.execute("DELETE FROM retained_messages WHERE topic = ?1", [&topic]);
        })
        .await
        .unwrap();
    }

    pub async fn load_retained(&self) -> Result<Vec<(String, Vec<u8>, u8)>, rusqlite::Error> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let mut stmt = conn.prepare("SELECT topic, payload, qos FROM retained_messages")?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            let mut result = Vec::new();
            for row in rows {
                result.push(row?);
            }
            Ok(result)
        })
        .await
        .unwrap()
    }

    pub async fn save_qos2_incoming(
        &self,
        client_id: String,
        packet_id: u16,
        topic: String,
        payload: Vec<u8>,
        retain: bool,
    ) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            if let Err(e) = conn.execute(
                "INSERT OR REPLACE INTO qos2_incoming (client_id, packet_id, topic, payload, retain) VALUES (?1, ?2, ?3, ?4, ?5)",
                (&client_id, packet_id, &topic, &payload, i32::from(retain)),
            ) { error!("Failed to save inbound QoS2 state for {}:{}: {:?}", client_id, packet_id, e); }
        }).await.unwrap();
    }

    pub async fn delete_qos2_incoming(&self, client_id: String, packet_id: u16) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let _ = conn.execute(
                "DELETE FROM qos2_incoming WHERE client_id = ?1 AND packet_id = ?2",
                (&client_id, packet_id),
            );
        })
        .await
        .unwrap();
    }

    pub async fn load_qos2_incoming(
        &self,
    ) -> Result<Vec<(String, u16, String, Vec<u8>, bool)>, rusqlite::Error> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let mut stmt = conn.prepare(
                "SELECT client_id, packet_id, topic, payload, retain FROM qos2_incoming",
            )?;
            let rows = stmt.query_map([], |row| {
                let retain: i32 = row.get(4)?;
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    retain != 0,
                ))
            })?;
            let mut result = Vec::new();
            for row in rows {
                result.push(row?);
            }
            Ok(result)
        })
        .await
        .unwrap()
    }

    pub async fn save_qos2_outgoing(
        &self,
        client_id: String,
        packet_id: u16,
        topic: String,
        payload: Vec<u8>,
        retain: bool,
        subscription_identifier: Option<u32>,
        phase: u8,
    ) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            if let Err(e) = conn.execute(
                "INSERT OR REPLACE INTO qos2_outgoing (client_id, packet_id, topic, payload, retain, subscription_identifier, phase) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (&client_id, packet_id, &topic, &payload, i32::from(retain), subscription_identifier, phase),
            ) { error!("Failed to save outbound QoS2 state for {}:{}: {:?}", client_id, packet_id, e); }
        }).await.unwrap();
    }

    pub async fn delete_qos2_outgoing(&self, client_id: String, packet_id: u16) {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let _ = conn.execute(
                "DELETE FROM qos2_outgoing WHERE client_id = ?1 AND packet_id = ?2",
                (&client_id, packet_id),
            );
        })
        .await
        .unwrap();
    }

    pub async fn load_qos2_outgoing(
        &self,
    ) -> Result<Vec<(String, u16, String, Vec<u8>, bool, Option<u32>, u8)>, rusqlite::Error> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let mut stmt = conn.prepare("SELECT client_id, packet_id, topic, payload, retain, subscription_identifier, phase FROM qos2_outgoing")?;
            let rows = stmt.query_map([], |row| {
                let retain: i32 = row.get(4)?;
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, retain != 0, row.get(5)?, row.get(6)?))
            })?;
            let mut result = Vec::new();
            for row in rows { result.push(row?); }
            Ok(result)
        }).await.unwrap()
    }
}
