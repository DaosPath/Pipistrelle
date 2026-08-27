# MQTT 5.0 compliance status

This file is the explicit compliance ledger for Pipistrelle V2. A feature being listed in the README does **not** mean the broker claims complete MQTT 5.0 conformance.

Status meanings:

- **Implemented** — exercised by unit/integration or raw-protocol tests in the repository.
- **Partial** — useful behavior exists, but one or more normative cases or negotiated limits remain.
- **Not yet** — intentionally tracked work; do not rely on it.

## Application Message path

| Area | Status | Notes |
|---|---|---|
| QoS 0 | Implemented | Bounded routing path plus zero-route ingest fast path. |
| QoS 1 | Implemented | PUBACK, persistent outbound state and retry on Session resume. |
| QoS 2 | Implemented | PUBLISH/PUBREC/PUBREL/PUBCOMP, inbound dedupe and persistent inbound/outbound recovery. |
| Retained messages | Implemented | Store/replace/delete, SQLite recovery, Retain Handling 0/1/2 and RAP. |
| Payload Format Indicator | Implemented | Preserved end-to-end for PUBLISH and Will. |
| Message Expiry Interval | Implemented | Remaining interval is reduced by broker wait time; undelivered expired copies are discarded. |
| Content Type | Implemented | Preserved unaltered. |
| Response Topic | Implemented | Preserved; invalid/wildcard Response Topic rejected. |
| Correlation Data | Implemented | Preserved byte-for-byte. |
| User Properties | Implemented | All pairs preserved, including duplicates and order. |
| Subscription Identifier | Implemented | Generated from matching subscriptions; illegal Client->Server use is rejected. |
| Topic Alias | Partial / inbound implemented | Client→Server aliases support establish/use/update semantics, zero-length Topic Name reuse, per-Network-Connection reset, range checks and protocol errors. Alias mappings are never forwarded to subscribers. Server→Client alias emission is not implemented yet. |

## Last Will and Session state

| Area | Status | Notes |
|---|---|---|
| Will QoS / Retain | Implemented | QoS 0/1/2 routing and retained Will behavior. |
| Will Delay | Implemented | Delayed publication, Session Expiry minimum and reconnect cancellation. |
| Will Application Properties | Implemented | PFI, expiry, content type, response topic, correlation data and ordered user properties. |
| Will persistence across abrupt process failure | Implemented | Will row is durable before CONNACK; `SIGKILL` restart tests cover active and delayed Wills. |
| Will recovery timing after server failure | Partial by design | For an active connection killed with the server before a durable disconnect timestamp exists, restart is the observed loss point for a still-delayed Will. MQTT 5 permits server-failure Will publication to be deferred until restart. |
| Session Present / Session Expiry | Implemented | Persistent state and expiry timers reconstructed across restart. |
| Offline QoS1/QoS2 | Implemented | Message properties and expiry metadata survive restart. |
| ClientID takeover | Implemented | Previous connection receives DISCONNECT 0x8E; cleanup cannot erase replacement. |
| Principal-bound persistent state | Pipistrelle hardening | A different authenticated ACL principal cannot inherit the same ClientID Session/Will state. |

## Subscriptions

| Area | Status | Notes |
|---|---|---|
| Exact and wildcard subscriptions | Implemented | Trie plus exact-route cache. |
| No Local | Implemented | Includes protocol rejection for invalid shared-subscription use. |
| Retain As Published | Implemented | Live RETAIN behavior follows subscription option. |
| Retain Handling | Implemented | Values 0, 1 and 2. |
| Shared subscriptions | Partial | Routing exists; fairness and broader edge-case conformance remain active work. |
| UNSUBSCRIBE / UNSUBACK | Implemented | Full MQTT v5 packet path, ordered per-filter reason codes, durable subscription deletion before UNSUBACK, and SIGKILL/restart recovery tests. |

## Negotiated limits and control-plane gaps

| Area | Status | Notes |
|---|---|---|
| Authentication + ACL | Implemented (Pipistrelle policy) | Argon2id credentials, fail-closed auth and topic ACLs. |
| Enhanced AUTH exchange | Not yet | CONNECT authentication-method state machine is not claimed complete. |
| Receive Maximum enforcement | Implemented | Server advertises a configurable inbound limit; QoS1/2 inbound credits are held until PUBACK/PUBREC bytes are written. Peer Receive Maximum gates outbound QoS1/2 and preserves queue order across restart. |
| Maximum Packet Size enforcement | Implemented | Oversized inbound packets are rejected from the fixed-header length before full buffering; outbound packets respect the client limit and CONNACK is reduced to a minimal legal form when needed. |
| Topic Alias Maximum | Partial / inbound implemented | CONNACK advertises configurable `PIPISTRELLE_TOPIC_ALIAS_MAXIMUM` (default 32) and enforces it for Client→Server PUBLISH. CONNECT Topic Alias Maximum is decoded, but Pipistrelle does not yet emit Server→Client aliases. |
| Server-assigned zero-length ClientID | Implemented | Empty CONNECT ClientID receives Assigned Client Identifier in CONNACK and can use it to resume a persistent Session. |
| Strict MQTT UTF-8 rules | Implemented core rules | Rejects malformed UTF-8, U+0000, disallowed control/noncharacter code points; packet/filter tests cover malformed cases. Exhaustive external conformance/fuzz corpus remains separate work. |
| Complete malformed-packet/error matrix | Partial | Important property/protocol errors are tested; full corpus/fuzz conformance remains work. |

## Test gates

- `cargo test --all-targets --locked` — Rust codec/router/session tests.
- `test_broker.py` — TCP, auth, ACL, TLS/PQC, WebSocket and metrics integration.
- `test_protocol_v2.py` — raw MQTT v5 packet/state tests, including Application Message properties, Message Expiry, Topic Alias lifecycle/reset, UNSUBSCRIBE, negotiated limits, assigned ClientIDs, TCP-fragmented CONNECT, UTF-8 and varint errors.
- `test_protocol_restart_v2.py` — **destructive local Docker test** which repeatedly sends `SIGKILL` to the broker and validates durable Wills, retained state, QoS1/QoS2 recovery, durable UNSUBSCRIBE and ordered Receive Maximum queues.

The destructive suite must only be used on a development/test broker.
