use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionInfo {
    pub client_id: String,
    pub qos: u8,
    pub subscription_identifier: Option<u32>,
    pub no_local: bool,
    pub retain_as_published: bool,
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
    exact_routes: RwLock<HashMap<String, Arc<[SubscriptionInfo]>>>,
    active_routes: AtomicUsize,
    non_exact_routes: AtomicUsize,
    mutation_epoch: AtomicU64,
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
            exact_routes: RwLock::new(HashMap::new()),
            active_routes: AtomicUsize::new(0),
            non_exact_routes: AtomicUsize::new(0),
            mutation_epoch: AtomicU64::new(0),
        }
    }

    #[inline]
    fn is_exact_normal_filter(topic_filter: &str) -> bool {
        !topic_filter.starts_with("$share/")
            && !topic_filter.as_bytes().contains(&b'+')
            && !topic_filter.as_bytes().contains(&b'#')
    }

    fn update_exact_route(&self, topic: &str, sub_info: SubscriptionInfo) {
        let mut routes = self.exact_routes.write();
        let mut entries = routes
            .get(topic)
            .map(|entries| entries.as_ref().to_vec())
            .unwrap_or_default();
        entries.retain(|entry| entry.client_id != sub_info.client_id);
        entries.push(sub_info);
        routes.insert(topic.to_string(), Arc::from(entries));
    }

    fn remove_exact_route(&self, topic: &str, client_id: &str) -> bool {
        let mut routes = self.exact_routes.write();
        let Some(current) = routes.get(topic) else {
            return false;
        };
        let mut entries = current.as_ref().to_vec();
        let before = entries.len();
        entries.retain(|entry| entry.client_id != client_id);
        if entries.len() == before {
            return false;
        }
        if entries.is_empty() {
            routes.remove(topic);
        } else {
            routes.insert(topic.to_string(), Arc::from(entries));
        }
        true
    }

    /// Subscribes with default MQTT v5 options (No Local=0, RAP=0).
    pub fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &str,
        qos: u8,
        subscription_identifier: Option<u32>,
    ) {
        self.subscribe_with_options(
            client_id,
            topic_filter,
            qos,
            subscription_identifier,
            false,
            false,
        );
    }

    /// Subscribes a client to a topic filter with MQTT v5 routing options.
    /// Supports wildcards (`+`, `#`) and Shared Subscriptions (`$share/group/topic_filter`).
    pub fn subscribe_with_options(
        &self,
        client_id: &str,
        topic_filter: &str,
        qos: u8,
        subscription_identifier: Option<u32>,
        no_local: bool,
        retain_as_published: bool,
    ) {
        let mut root = self.root.write();
        let (group, filter) = parse_shared_subscription(topic_filter);
        let segments: Vec<&str> = filter.split('/').collect();

        let mut current = &mut *root;
        for &segment in &segments {
            match segment {
                "+" => {
                    current = current
                        .plus_child
                        .get_or_insert_with(|| Box::new(TrieNode::default()));
                }
                "#" => {
                    current = current
                        .hash_child
                        .get_or_insert_with(|| Box::new(TrieNode::default()));
                    break; // '#' must be the last segment, so we stop here
                }
                _ => {
                    current = current
                        .children
                        .entry(segment.to_string())
                        .or_insert_with(TrieNode::default);
                }
            }
        }

        let sub_info = SubscriptionInfo {
            client_id: client_id.to_string(),
            qos,
            subscription_identifier,
            no_local,
            retain_as_published,
        };

        let inserted_new = if let Some(grp) = group {
            let list = current
                .shared_subscriptions
                .entry(grp.to_string())
                .or_default();
            let existed = list.iter().any(|s| s.client_id == client_id);
            // Avoid duplicate registrations for the same client in the same group.
            list.retain(|s| s.client_id != client_id);
            list.push(sub_info.clone());
            !existed
        } else {
            current
                .subscriptions
                .insert(client_id.to_string(), sub_info.clone())
                .is_none()
        };
        if Self::is_exact_normal_filter(topic_filter) {
            self.update_exact_route(topic_filter, sub_info.clone());
        }
        if inserted_new {
            self.active_routes.fetch_add(1, Ordering::Release);
            if !Self::is_exact_normal_filter(topic_filter) {
                self.non_exact_routes.fetch_add(1, Ordering::Release);
            }
        }
        // Any subscribe/upsert can change routing semantics (QoS/options included).
        // Fast zero-route batching snapshots this epoch and falls back if it changes.
        self.mutation_epoch.fetch_add(1, Ordering::Release);
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
                        let (rem, empty) =
                            unsubscribe_recursive(child, client_id, &segments[1..], group);
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
                        let (rem, empty) =
                            unsubscribe_recursive(child, client_id, &segments[1..], group);
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
                        let (rem, empty) =
                            unsubscribe_recursive(child, client_id, &segments[1..], group);
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
        if removed {
            self.active_routes.fetch_sub(1, Ordering::Release);
            if Self::is_exact_normal_filter(topic_filter) {
                self.remove_exact_route(topic_filter, client_id);
            } else {
                self.non_exact_routes.fetch_sub(1, Ordering::Release);
            }
            self.mutation_epoch.fetch_add(1, Ordering::Release);
        }
        removed
    }

    /// Removes every normal/shared subscription owned by a client.
    /// Used when a clean session ends so stale routing entries cannot accumulate.
    pub fn remove_client(&self, client_id: &str) -> usize {
        fn remove_recursive(node: &mut TrieNode, client_id: &str) -> usize {
            let mut removed = usize::from(node.subscriptions.remove(client_id).is_some());

            node.shared_subscriptions.retain(|_, list| {
                let before = list.len();
                list.retain(|sub| sub.client_id != client_id);
                removed += before - list.len();
                !list.is_empty()
            });

            node.children.retain(|_, child| {
                removed += remove_recursive(child, client_id);
                !child.is_empty()
            });

            if let Some(child) = node.plus_child.as_mut() {
                removed += remove_recursive(child, client_id);
                if child.is_empty() {
                    node.plus_child = None;
                }
            }
            if let Some(child) = node.hash_child.as_mut() {
                removed += remove_recursive(child, client_id);
                if child.is_empty() {
                    node.hash_child = None;
                }
            }

            removed
        }

        let mut root = self.root.write();
        let removed = remove_recursive(&mut root, client_id);
        if removed > 0 {
            let exact_removed = {
                let mut routes = self.exact_routes.write();
                let topics: Vec<String> = routes.keys().cloned().collect();
                let mut count = 0usize;
                for topic in topics {
                    if let Some(current) = routes.get(&topic) {
                        let mut entries = current.as_ref().to_vec();
                        let before = entries.len();
                        entries.retain(|entry| entry.client_id != client_id);
                        count += before - entries.len();
                        if entries.is_empty() {
                            routes.remove(&topic);
                        } else if entries.len() != before {
                            routes.insert(topic, Arc::from(entries));
                        }
                    }
                }
                count
            };
            let non_exact_removed = removed.saturating_sub(exact_removed);
            self.active_routes.fetch_sub(removed, Ordering::Release);
            if non_exact_removed > 0 {
                self.non_exact_routes
                    .fetch_sub(non_exact_removed, Ordering::Release);
            }
            self.mutation_epoch.fetch_add(1, Ordering::Release);
        }
        removed
    }

    /// Matches a publish topic against all active subscriptions.
    /// Resolves wildcards (`+`, `#`) and aggregates results.
    #[inline]
    pub fn has_routes(&self) -> bool {
        self.active_routes.load(Ordering::Acquire) != 0
    }

    #[inline]
    pub fn mutation_epoch(&self) -> u64 {
        self.mutation_epoch.load(Ordering::Acquire)
    }

    #[inline]
    pub fn has_only_exact_routes(&self) -> bool {
        self.has_routes() && self.non_exact_routes.load(Ordering::Acquire) == 0
    }

    #[inline]
    pub fn match_exact(&self, topic: &str) -> Option<Arc<[SubscriptionInfo]>> {
        self.exact_routes.read().get(topic).cloned()
    }

    pub fn match_topic(&self, topic: &str) -> RouteResult {
        let root = self.root.read();
        let segments: Vec<&str> = topic.split('/').collect();
        let mut result = RouteResult::default();

        fn match_recursive(node: &TrieNode, segments: &[&str], result: &mut RouteResult) {
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

/// Matches a concrete MQTT topic name against a subscription filter.
/// Used for retained-message replay where the retained set is small and mutations are rare.
pub fn topic_matches_filter(topic: &str, filter: &str) -> bool {
    let (_, filter) = parse_shared_subscription(filter);
    let mut topics = topic.split('/');
    for level in filter.split('/') {
        if level == "#" {
            return true;
        }
        let Some(topic_level) = topics.next() else {
            return false;
        };
        if level != "+" && level != topic_level {
            return false;
        }
    }
    topics.next().is_none()
}

/// Validates MQTT v5 Topic Filter wildcard placement and Shared Subscription syntax.
pub fn topic_filter_valid(topic_filter: &str) -> bool {
    if topic_filter.is_empty() {
        return false;
    }
    let filter = if let Some(rest) = topic_filter.strip_prefix("$share/") {
        let Some((group, filter)) = rest.split_once('/') else {
            return false;
        };
        if group.is_empty()
            || group
                .as_bytes()
                .iter()
                .any(|b| matches!(*b, b'/' | b'+' | b'#'))
            || filter.is_empty()
        {
            return false;
        }
        filter
    } else {
        topic_filter
    };

    let levels: Vec<&str> = filter.split('/').collect();
    for (index, level) in levels.iter().enumerate() {
        if level.as_bytes().contains(&b'#') && (*level != "#" || index + 1 != levels.len()) {
            return false;
        }
        if level.as_bytes().contains(&b'+') && *level != "+" {
            return false;
        }
    }
    true
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
    fn test_exact_route_cache_lifecycle() {
        let router = TopicRouter::new();
        router.subscribe("client1", "sensor/temp", 0, None);
        assert!(router.has_only_exact_routes());
        let exact = router.match_exact("sensor/temp").unwrap();
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].client_id, "client1");

        router.subscribe("client2", "sensor/temp", 1, Some(7));
        let exact = router.match_exact("sensor/temp").unwrap();
        assert_eq!(exact.len(), 2);
        assert!(exact.iter().any(|sub| sub.client_id == "client2"));

        assert!(router.unsubscribe("client1", "sensor/temp"));
        let exact = router.match_exact("sensor/temp").unwrap();
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].client_id, "client2");

        assert_eq!(router.remove_client("client2"), 1);
        assert!(router.match_exact("sensor/temp").is_none());
        assert!(!router.has_routes());
    }

    #[test]
    fn routing_epoch_changes_for_route_semantic_mutations() {
        let router = TopicRouter::new();
        let e0 = router.mutation_epoch();
        router.subscribe_with_options("client1", "sensor/temp", 0, None, false, false);
        let e1 = router.mutation_epoch();
        assert!(e1 > e0);

        // Re-subscribing the same route can change QoS/options even though the route
        // count stays constant, so it must still invalidate zero-route snapshots.
        router.subscribe_with_options("client1", "sensor/temp", 1, Some(7), true, true);
        let e2 = router.mutation_epoch();
        assert!(e2 > e1);

        assert!(router.unsubscribe("client1", "sensor/temp"));
        assert!(router.mutation_epoch() > e2);
    }

    #[test]
    fn wildcard_route_disables_exact_only_fast_path() {
        let router = TopicRouter::new();
        router.subscribe("client1", "sensor/temp", 0, None);
        assert!(router.has_only_exact_routes());
        router.subscribe("client2", "sensor/+", 0, None);
        assert!(!router.has_only_exact_routes());
        assert!(router.unsubscribe("client2", "sensor/+"));
        assert!(router.has_only_exact_routes());
    }

    #[test]
    fn topic_filter_validation_covers_wildcards_and_shared_syntax() {
        for valid in [
            "a/b",
            "a/+",
            "a/#",
            "+/b",
            "#",
            "$share/g/a/+",
            "$share/g/#",
        ] {
            assert!(topic_filter_valid(valid), "expected valid: {valid}");
        }
        for invalid in [
            "",
            "a#",
            "a/#/b",
            "a/+b",
            "$share//a",
            "$share/g/",
            "$share/g+/a",
        ] {
            assert!(!topic_filter_valid(invalid), "expected invalid: {invalid}");
        }
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

    #[test]
    fn test_remove_client_cleans_all_routes() {
        let router = TopicRouter::new();
        router.subscribe("client-a", "sensor/+", 0, None);
        router.subscribe("client-a", "$share/workers/jobs/#", 1, None);
        router.subscribe("client-b", "sensor/+", 0, None);

        assert_eq!(router.remove_client("client-a"), 2);

        let sensor = router.match_topic("sensor/temp");
        assert_eq!(sensor.normal.len(), 1);
        assert_eq!(sensor.normal[0].client_id, "client-b");

        let jobs = router.match_topic("jobs/one");
        assert!(jobs.shared.is_empty());
        assert!(router.has_routes());

        assert_eq!(router.remove_client("client-b"), 1);
        assert!(!router.has_routes());
    }
}
