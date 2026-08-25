use argon2::{Argon2, PasswordHash, PasswordVerifier};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
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
}

impl AuthConfig {
    fn closed() -> Self {
        Self {
            users: Some(HashMap::new()),
        }
    }

    fn anonymous() -> Self {
        Self { users: None }
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
                        info!("Loaded {} user(s) from {}", map.len(), path_str);
                        Self { users: Some(map) }
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

    /// Authenticates a user with Argon2id.
    /// Anonymous access is allowed only when explicitly enabled.
    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        let users = match &self.users {
            Some(u) => u,
            None => return true,
        };

        users
            .get(username)
            .map(|user| verify_password(password, &user.password_hash))
            .unwrap_or(false)
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
    let topic_levels: Vec<&str> = topic.split('/').collect();
    let filter_levels: Vec<&str> = filter.split('/').collect();

    let mut i = 0;
    while i < filter_levels.len() {
        if filter_levels[i] == "#" {
            return true;
        }
        if i >= topic_levels.len() {
            return false;
        }
        if filter_levels[i] != "+" && filter_levels[i] != topic_levels[i] {
            return false;
        }
        i += 1;
    }

    i == topic_levels.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_argon2_password_verification() {
        let hash = "$argon2id$v=19$m=65536,t=3,p=1$ZDhmMWVlOGMxMTZlOTAwYzJhZGIzMDBlYWJjZWJlZmY$r+HP8qLT3CpBzxqniOJShpDgZ/O95L8TaQGBQKB573o";
        assert!(verify_password("admin123", hash));
        assert!(!verify_password("wrongpassword", hash));
    }

    #[test]
    fn test_topic_matches_acl_filter() {
        assert!(topic_matches_acl_filter("a/b/c", "a/b/c"));
        assert!(!topic_matches_acl_filter("a/b/c", "a/b/d"));
        assert!(topic_matches_acl_filter("sensor/temperature", "sensor/+"));
        assert!(topic_matches_acl_filter("sensor/temperature/cpu", "sensor/+/cpu"));
        assert!(!topic_matches_acl_filter("sensor/temperature/cpu", "sensor/+"));
        assert!(topic_matches_acl_filter("sensor/temperature/cpu", "sensor/#"));
        assert!(topic_matches_acl_filter("sensor", "sensor/#"));
        assert!(topic_matches_acl_filter("a/b/c/d/e", "#"));
    }
}
