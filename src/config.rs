use serde::Deserialize;
use sha2::{Sha256, Digest};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tracing::{info, warn, error};

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
    // Cache users in a HashMap for O(1) lookup
    users: Option<HashMap<String, UserConfig>>,
}

impl AuthConfig {
    pub fn load() -> Self {
        let path = Path::new("credentials.json");
        if !path.exists() {
            warn!("credentials.json not found. Running in Open Access (Anonymous) mode.");
            return Self { users: None };
        }

        match File::open(path) {
            Ok(mut file) => {
                let mut content = String::new();
                if let Err(e) = file.read_to_string(&mut content) {
                    error!("Failed to read credentials.json: {:?}", e);
                    return Self { users: None };
                }

                match serde_json::from_str::<CredentialsConfig>(&content) {
                    Ok(config) => {
                        let mut map = HashMap::new();
                        for user in config.users {
                            map.insert(user.username.clone(), user);
                        }
                        info!("Loaded {} user(s) from credentials.json", map.len());
                        Self { users: Some(map) }
                    }
                    Err(e) => {
                        error!("Failed to parse credentials.json: {:?}", e);
                        Self { users: None }
                    }
                }
            }
            Err(e) => {
                error!("Failed to open credentials.json: {:?}", e);
                Self { users: None }
            }
        }
    }

    /// Authenticates a user with a given password.
    /// Returns true if authentication succeeds or if open access is enabled.
    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        let users = match &self.users {
            Some(u) => u,
            None => return true, // Open access mode
        };

        if let Some(user) = users.get(username) {
            let input_hash = hash_password(password);
            input_hash == user.password_hash
        } else {
            false
        }
    }

    /// Authorizes an action ("read" or "write") for a user on a given topic.
    pub fn authorize(&self, username: &str, topic: &str, action: &str) -> bool {
        let users = match &self.users {
            Some(u) => u,
            None => return true, // Open access mode
        };

        if let Some(user) = users.get(username) {
            for rule in &user.acl {
                // 1. Check if the rule supports the requested action
                let action_allowed = match rule.access.as_str() {
                    "readwrite" => true,
                    "read" => action == "read",
                    "write" => action == "write",
                    _ => false,
                };

                if action_allowed {
                    // 2. Check if the topic matches the rule's topic filter
                    if topic_matches_acl_filter(topic, &rule.topic) {
                        return true;
                    }
                }
            }
            false
        } else {
            false
        }
    }
}

fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    let result = hasher.finalize();
    format!("{:x}", result)
}

/// Matches a publish topic or subscription topic filter against an ACL filter pattern.
/// E.g. topic "sensor/opi_zero" matches filter "sensor/+" and "sensor/#"
pub fn topic_matches_acl_filter(topic: &str, filter: &str) -> bool {
    let topic_levels: Vec<&str> = topic.split('/').collect();
    let filter_levels: Vec<&str> = filter.split('/').collect();
    
    let mut i = 0;
    while i < filter_levels.len() {
        if filter_levels[i] == "#" {
            return true; // Wildcard matches all remaining levels
        }
        if i >= topic_levels.len() {
            return false; // Topic is too short for the filter
        }
        if filter_levels[i] != "+" && filter_levels[i] != topic_levels[i] {
            return false; // Specific levels do not match
        }
        i += 1;
    }
    
    // Exact length match required if there was no trailing '#'
    i == topic_levels.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_password() {
        assert_eq!(
            hash_password("admin123"),
            "240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9"
        );
    }

    #[test]
    fn test_topic_matches_acl_filter() {
        // Exact match
        assert!(topic_matches_acl_filter("a/b/c", "a/b/c"));
        assert!(!topic_matches_acl_filter("a/b/c", "a/b/d"));

        // Single-level wildcard (+)
        assert!(topic_matches_acl_filter("sensor/temperature", "sensor/+"));
        assert!(topic_matches_acl_filter("sensor/temperature/cpu", "sensor/+/cpu"));
        assert!(!topic_matches_acl_filter("sensor/temperature/cpu", "sensor/+"));

        // Multi-level wildcard (#)
        assert!(topic_matches_acl_filter("sensor/temperature/cpu", "sensor/#"));
        assert!(topic_matches_acl_filter("sensor", "sensor/#"));
        assert!(topic_matches_acl_filter("a/b/c/d/e", "#"));
    }
}
