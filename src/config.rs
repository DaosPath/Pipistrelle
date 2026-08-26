use argon2::{Argon2, PasswordHash, PasswordVerifier};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

#[derive(Deserialize, Debug, Clone)]
pub struct AclRule {
    pub topic: String,
    pub access: String, // "read", "write", "readwrite"
}

#[derive(Deserialize, Debug, Clone)]
pub struct UserConfig {
    pub username: String,
    pub password_hash: String,
    pub acl: Vec<AclRule>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CredentialsConfig {
    pub users: Vec<UserConfig>,
}

pub struct AuthConfig {
    // None means anonymous mode was explicitly enabled.
    // Some(map), including an empty map, means authentication is enforced.
    users: Option<HashMap<String, UserConfig>>,
    // Argon2 is intentionally expensive. Bound concurrent verifications to protect RAM/CPU.
    auth_limit: Arc<Semaphore>,
}

impl AuthConfig {
    fn with_users(users: Option<HashMap<String, UserConfig>>) -> Self {
        Self {
            users,
            auth_limit: Arc::new(Semaphore::new(max_auth_concurrency())),
        }
    }

    fn closed() -> Self {
        Self::with_users(Some(HashMap::new()))
    }

    fn anonymous() -> Self {
        Self::with_users(None)
    }

    pub fn load() -> Self {
        let path_str = std::env::var("PIPISTRELLE_CREDENTIALS_PATH")
            .unwrap_or_else(|_| "credentials.json".to_string());
        let path = Path::new(&path_str);

        if !path.exists() {
            if anonymous_access_enabled() {
                warn!(
                    "Credentials file {:?} not found. Anonymous access was explicitly enabled.",
                    path
                );
                return Self::anonymous();
            }

            error!(
                "Credentials file {:?} not found. Authentication is fail-closed; all clients will be rejected.",
                path
            );
            return Self::closed();
        }

        match File::open(path) {
            Ok(mut file) => {
                let mut content = String::new();
                if let Err(e) = file.read_to_string(&mut content) {
                    error!(
                        "Failed to read credentials file {:?}: {:?}. Authentication remains closed.",
                        path, e
                    );
                    return Self::closed();
                }

                match serde_json::from_str::<CredentialsConfig>(&content) {
                    Ok(config) => {
                        let mut map = HashMap::new();
                        for user in config.users {
                            map.insert(user.username.clone(), user);
                        }
                        info!(
                            "Loaded {} user(s) from {} (max concurrent Argon2 verifications: {})",
                            map.len(),
                            path_str,
                            max_auth_concurrency(),
                        );
                        Self::with_users(Some(map))
                    }
                    Err(e) => {
                        error!(
                            "Failed to parse credentials file {}: {:?}. Authentication remains closed.",
                            path_str, e
                        );
                        Self::closed()
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to open credentials file {}: {:?}. Authentication remains closed.",
                    path_str, e
                );
                Self::closed()
            }
        }
    }

    /// Authenticates a user with Argon2id outside Tokio's async worker threads.
    /// Anonymous access is allowed only when explicitly enabled.
    pub async fn authenticate(&self, username: &str, password: &str) -> bool {
        let users = match &self.users {
            Some(u) => u,
            None => return true,
        };

        let Some(user) = users.get(username) else {
            return false;
        };

        let hash = user.password_hash.clone();
        let password = password.to_string();
        let Ok(permit) = self.auth_limit.clone().acquire_owned().await else {
            return false;
        };

        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            verify_password(&password, &hash)
        })
        .await
        .unwrap_or(false)
    }

    /// Returns true when a session can cache unconditional access for an action.
    /// This removes user-map and ACL traversal work from the per-packet hot path
    /// for common administrative/service accounts using a `#` ACL.
    pub fn authorizes_all(&self, username: &str, action: &str) -> bool {
        let users = match &self.users {
            Some(users) => users,
            None => return true,
        };
        let Some(user) = users.get(username) else {
            return false;
        };
        user.acl.iter().any(|rule| {
            if rule.topic != "#" {
                return false;
            }
            match rule.access.as_str() {
                "readwrite" => true,
                "read" => action == "read",
                "write" => action == "write",
                _ => false,
            }
        })
    }

    /// Authorizes an action ("read" or "write") for a user on a given topic.
    pub fn authorize(&self, username: &str, topic: &str, action: &str) -> bool {
        let users = match &self.users {
            Some(u) => u,
            None => return true,
        };

        if let Some(user) = users.get(username) {
            for rule in &user.acl {
                let action_allowed = match rule.access.as_str() {
                    "readwrite" => true,
                    "read" => action == "read",
                    "write" => action == "write",
                    _ => false,
                };

                if action_allowed && topic_matches_acl_filter(topic, &rule.topic) {
                    return true;
                }
            }
        }

        false
    }
}

fn max_auth_concurrency() -> usize {
    std::env::var("PIPISTRELLE_MAX_AUTH_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .map(|value| value.clamp(1, 64))
        .unwrap_or(4)
}

fn anonymous_access_enabled() -> bool {
    std::env::var("PIPISTRELLE_ALLOW_ANONYMOUS")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn verify_password(password: &str, encoded_hash: &str) -> bool {
    let Ok(parsed_hash) = PasswordHash::new(encoded_hash) else {
        return false;
    };

    Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok()
}

/// Matches a publish topic or subscription topic filter against an ACL filter pattern.
/// E.g. topic "sensor/opi_zero" matches filter "sensor/+" and "sensor/#"
pub fn topic_matches_acl_filter(topic: &str, filter: &str) -> bool {
    // Common ACLs should be nearly free in the publish hot path.
    if filter == "#" {
        return true;
    }
    if !filter.as_bytes().contains(&b'+') && !filter.as_bytes().contains(&b'#') {
        return topic == filter;
    }

    // Walk both strings directly. No Vec allocation per authorization check.
    let mut topic_levels = topic.split('/');
    for filter_level in filter.split('/') {
        if filter_level == "#" {
            return true;
        }
        let Some(topic_level) = topic_levels.next() else {
            return false;
        };
        if filter_level != "+" && filter_level != topic_level {
            return false;
        }
    }
    topic_levels.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_argon2_password_verification() {
        let hash = "$argon2id$v=19$m=19456,t=2,p=1$MjY4NGRlMWY5YzliZDUwNjBhOGJhYTJhZDM1OWViZTE$qjlHo3ALKgMXLw1bgRz/p7LGhqvC4RKrgxMuvgPNNfg";
        assert!(verify_password("admin123", hash));
        assert!(!verify_password("wrongpassword", hash));
    }

    #[test]
    fn test_global_acl_cache_detection() {
        let user = UserConfig {
            username: "admin".to_string(),
            password_hash: "unused".to_string(),
            acl: vec![AclRule {
                topic: "#".to_string(),
                access: "readwrite".to_string(),
            }],
        };
        let mut users = HashMap::new();
        users.insert(user.username.clone(), user);
        let auth = AuthConfig::with_users(Some(users));
        assert!(auth.authorizes_all("admin", "read"));
        assert!(auth.authorizes_all("admin", "write"));
        assert!(!auth.authorizes_all("missing", "write"));
    }

    #[test]
    fn test_topic_matches_acl_filter() {
        assert!(topic_matches_acl_filter("a/b/c", "a/b/c"));
        assert!(!topic_matches_acl_filter("a/b/c", "a/b/d"));
        assert!(topic_matches_acl_filter("sensor/temperature", "sensor/+"));
        assert!(topic_matches_acl_filter(
            "sensor/temperature/cpu",
            "sensor/+/cpu"
        ));
        assert!(!topic_matches_acl_filter(
            "sensor/temperature/cpu",
            "sensor/+"
        ));
        assert!(topic_matches_acl_filter(
            "sensor/temperature/cpu",
            "sensor/#"
        ));
        assert!(topic_matches_acl_filter("sensor", "sensor/#"));
        assert!(topic_matches_acl_filter("a/b/c/d/e", "#"));
    }
}
