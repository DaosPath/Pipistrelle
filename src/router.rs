use std::collections::HashMap;
use parking_lot::RwLock;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionInfo {
    pub client_id: String,
    pub qos: u8,
    pub subscription_identifier: Option<u32>,
}

#[derive(Default, Debug)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    plus_child: Option<Box<TrieNode>>,
    hash_child: Option<Box<TrieNode>>,
    // Normal subscriptions: ClientID -> SubscriptionInfo
    subscriptions: HashMap<String, SubscriptionInfo>,
    // Shared subscriptions: GroupName -> Vec<SubscriptionInfo>
    shared_subscriptions: HashMap<String, Vec<SubscriptionInfo>>,
}

impl TrieNode {
    fn is_empty(&self) -> bool {
        self.children.is_empty()
            && self.plus_child.is_none()
            && self.hash_child.is_none()
            && self.subscriptions.is_empty()
            && self.shared_subscriptions.is_empty()
    }
}

pub struct TopicRouter {
    root: RwLock<TrieNode>,
}

/// Represents matching subscribers.
/// Normal subscribers receive messages individually.
/// Shared subscribers are grouped by group name so the broker can load-balance (round-robin) between them.
#[derive(Debug, Default)]
pub struct RouteResult {
    pub normal: Vec<SubscriptionInfo>,
    pub shared: HashMap<String, Vec<SubscriptionInfo>>,
}

impl TopicRouter {
    pub fn new() -> Self {
        Self {
            root: RwLock::new(TrieNode::default()),
        }
    }

    /// Subscribes a client to a topic filter.
    /// Supports wildcards (`+`, `#`) and Shared Subscriptions (`$share/group/topic_filter`).
    pub fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &str,
        qos: u8,
        subscription_identifier: Option<u32>,
    ) {
        let mut root = self.root.write();
        let (group, filter) = parse_shared_subscription(topic_filter);
        let segments: Vec<&str> = filter.split('/').collect();

        let mut current = &mut *root;
        for &segment in &segments {
            match segment {
                "+" => {
                    current = current.plus_child.get_or_insert_with(|| Box::new(TrieNode::default()));
                }
                "#" => {
                    current = current.hash_child.get_or_insert_with(|| Box::new(TrieNode::default()));
                    break; // '#' must be the last segment, so we stop here
                }
                _ => {
                    current = current.children.entry(segment.to_string()).or_insert_with(TrieNode::default);
                }
            }
        }

        let sub_info = SubscriptionInfo {
            client_id: client_id.to_string(),
            qos,
            subscription_identifier,
        };

        if let Some(grp) = group {
            let list = current.shared_subscriptions.entry(grp.to_string()).or_default();
            // Avoid duplicate registrations for the same client in the same group
            list.retain(|s| s.client_id != client_id);
            list.push(sub_info);
        } else {
            current.subscriptions.insert(client_id.to_string(), sub_info);
        }
    }

    /// Unsubscribes a client from a topic filter.
    /// Returns true if a subscription was actually removed.
    pub fn unsubscribe(&self, client_id: &str, topic_filter: &str) -> bool {
        let mut root = self.root.write();
        let (group, filter) = parse_shared_subscription(topic_filter);
        let segments: Vec<&str> = filter.split('/').collect();

        fn unsubscribe_recursive(
            node: &mut TrieNode,
            client_id: &str,
            segments: &[&str],
            group: Option<&str>,
        ) -> (bool, bool) {
            if segments.is_empty() {
                // Leaf reached
                let removed = if let Some(grp) = group {
                    if let Some(list) = node.shared_subscriptions.get_mut(grp) {
                        let original_len = list.len();
                        list.retain(|s| s.client_id != client_id);
                        let removed = list.len() < original_len;
                        if list.is_empty() {
                            node.shared_subscriptions.remove(grp);
                        }
                        removed
                    } else {
                        false
                    }
                } else {
                    node.subscriptions.remove(client_id).is_some()
                };
                return (removed, node.is_empty());
            }

            let segment = segments[0];
            let (removed, _child_empty) = match segment {
                "+" => {
                    if let Some(ref mut child) = node.plus_child {
                        let (rem, empty) = unsubscribe_recursive(child, client_id, &segments[1..], group);
                        if empty {
                            node.plus_child = None;
                        }
                        (rem, empty)
                    } else {
                        (false, false)
                    }
                }
                "#" => {
                    if let Some(ref mut child) = node.hash_child {
                        let (rem, empty) = unsubscribe_recursive(child, client_id, &segments[1..], group);
                        if empty {
                            node.hash_child = None;
                        }
                        (rem, empty)
                    } else {
                        (false, false)
                    }
                }
                _ => {
                    if let Some(child) = node.children.get_mut(segment) {
                        let (rem, empty) = unsubscribe_recursive(child, client_id, &segments[1..], group);
                        if empty {
                            node.children.remove(segment);
                        }
                        (rem, empty)
                    } else {
                        (false, false)
                    }
                }
            };

            (removed, node.is_empty())
        }

        let (removed, _) = unsubscribe_recursive(&mut *root, client_id, &segments, group);
        removed
    }

    /// Matches a publish topic against all active subscriptions.
    /// Resolves wildcards (`+`, `#`) and aggregates results.
    pub fn match_topic(&self, topic: &str) -> RouteResult {
        let root = self.root.read();
        let segments: Vec<&str> = topic.split('/').collect();
        let mut result = RouteResult::default();

        fn match_recursive(
            node: &TrieNode,
            segments: &[&str],
            result: &mut RouteResult,
        ) {
            // Check if there is a '#' wildcard at the current node.
            // '#' matches 0 or more remaining segments.
            if let Some(ref hash_child) = node.hash_child {
                collect_node_subscriptions(hash_child, result);
            }

            if segments.is_empty() {
                // Exact match leaf reached
                collect_node_subscriptions(node, result);
                return;
            }

            let segment = segments[0];

            // 1. Exact match
            if let Some(child) = node.children.get(segment) {
                match_recursive(child, &segments[1..], result);
            }

            // 2. Plus '+' wildcard match (matches exactly one level)
            if let Some(ref plus_child) = node.plus_child {
                match_recursive(plus_child, &segments[1..], result);
            }
        }

        match_recursive(&*root, &segments, &mut result);
        result
    }
}

fn collect_node_subscriptions(node: &TrieNode, result: &mut RouteResult) {
    for sub in node.subscriptions.values() {
        result.normal.push(sub.clone());
    }
    for (group, list) in &node.shared_subscriptions {
        let target = result.shared.entry(group.clone()).or_default();
        for sub in list {
            target.push(sub.clone());
        }
    }
}

/// Parses Shared Subscription prefix `$share/group/topic_filter`.
/// Returns (Some(group), topic_filter) or (None, topic_filter).
fn parse_shared_subscription(topic_filter: &str) -> (Option<&str>, &str) {
    if topic_filter.starts_with("$share/") {
        let parts: Vec<&str> = topic_filter.splitn(3, '/').collect();
        if parts.len() == 3 {
            return (Some(parts[1]), parts[2]);
        }
    }
    (None, topic_filter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_matches() {
        let router = TopicRouter::new();
        router.subscribe("client1", "sensor/temp", 1, None);
        router.subscribe("client2", "sensor/humi", 0, None);

        let res = router.match_topic("sensor/temp");
        assert_eq!(res.normal.len(), 1);
        assert_eq!(res.normal[0].client_id, "client1");

        let res2 = router.match_topic("sensor/humi");
        assert_eq!(res2.normal.len(), 1);
        assert_eq!(res2.normal[0].client_id, "client2");
    }

    #[test]
    fn test_plus_wildcard() {
        let router = TopicRouter::new();
        router.subscribe("client1", "sensor/+/cpu", 1, None);

        let res_match = router.match_topic("sensor/opi_zero3/cpu");
        assert_eq!(res_match.normal.len(), 1);
        assert_eq!(res_match.normal[0].client_id, "client1");

        let res_no_match = router.match_topic("sensor/opi_zero3/gpu/cpu");
        assert!(res_no_match.normal.is_empty());
    }

    #[test]
    fn test_hash_wildcard() {
        let router = TopicRouter::new();
        router.subscribe("client1", "sensor/#", 2, None);

        let res1 = router.match_topic("sensor/temp");
        assert_eq!(res1.normal.len(), 1);

        let res2 = router.match_topic("sensor/humi/status/errors");
        assert_eq!(res2.normal.len(), 1);

        let res3 = router.match_topic("sensor"); // '#' matches 0 or more levels
        assert_eq!(res3.normal.len(), 1);
    }

    #[test]
    fn test_shared_subscriptions() {
        let router = TopicRouter::new();
        router.subscribe("client1", "$share/workers/job/+", 1, None);
        router.subscribe("client2", "$share/workers/job/+", 1, None);
        router.subscribe("client3", "job/+", 0, None); // normal sub

        let res = router.match_topic("job/compile");
        assert_eq!(res.normal.len(), 1);
        assert_eq!(res.normal[0].client_id, "client3");

        let shared_workers = res.shared.get("workers").unwrap();
        assert_eq!(shared_workers.len(), 2);
        assert!(shared_workers.iter().any(|s| s.client_id == "client1"));
        assert!(shared_workers.iter().any(|s| s.client_id == "client2"));
    }

    #[test]
    fn test_unsubscribe() {
        let router = TopicRouter::new();
        router.subscribe("client1", "sensor/temp", 1, None);
        
        // Remove active sub
        assert!(router.unsubscribe("client1", "sensor/temp"));
        let res = router.match_topic("sensor/temp");
        assert!(res.normal.is_empty());

        // Remove non-existent sub
        assert!(!router.unsubscribe("client1", "sensor/temp"));
    }
}
