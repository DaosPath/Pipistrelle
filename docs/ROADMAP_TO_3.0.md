# Pipistrelle — Roadmap to 3.0.0.0

This document defines Pipistrelle's evolution from the current stable release 2.1.2.3 to 3.0.0.0.

The goal is to avoid artificial version jumps: every minor release in the 2.x series must represent a clearly completed product stage. Version 3.0.0.0 is reserved for the major architectural change: Pipistrelle as a distributed MQTT platform with stable clustering and high availability.

## Roadmap rules

- Public Pipistrelle versions use **four components**: MAJOR.MINOR.PATCH.BUILD.
- Cargo keeps standard three-component SemVer internally.
- A stage does not advance merely because a feature works; every release must pass protocol, persistence, security, integration and performance gates.
- MQTT correctness, backpressure, security and durability are never sacrificed to recover a benchmark.
- QoS 0 fast paths must remain isolated from work that belongs only to protocol-heavy paths.
- 3.0.0.0 is not published while clustering/HA remains experimental.

---

## Current baseline — 2.1.2.3

Current published state:

- MQTT v5 QoS 0/1/2.
- Retained messages.
- Last Will + Will Delay + crash persistence.
- Persistent Sessions + Session Expiry.
- ClientID takeover with DISCONNECT 0x8E.
- Principal-bound Session hardening.
- PUBLISH/Will application properties.
- Message Expiry.
- UNSUBSCRIBE / UNSUBACK.
- Bilateral Receive Maximum.
- Bilateral Maximum Packet Size.
- Server-assigned ClientID.
- Fragmented CONNECT parsing over TCP.
- Strict MQTT UTF-8 / variable-byte-integer validation.
- TLS 1.3 + hybrid PQC profile X25519MLKEM768.
- Prometheus, /health and /info.
- Bounded queues, backpressure and slow-consumer policy.
- Bounded bridge queue.
- Bidirectional per-Network-Connection Topic Alias: Client→Server maximum advertised in CONNACK and Server→Client maximum respecting the peer's CONNECT advertisement; connection-local mapping/remap/reset.
- Isolated ARM64/NEON QoS 0 ingest fast path.
- Optional SVE gather fast path for Topic Alias QoS 0 E2E on Linux/AArch64, with exact scalar fallback.
- Protocol/restart CI, RustSec, CodeQL and Dependabot engineering gates.

Orange Pi reference gates:

- Full-topic QoS 0 ingest: approximately 58 M msg/s on the validated native hot path. MQTT 5 Topic Alias: sustained 10B gate with a median of **163.550 M msg/s** on the normal host and a separate optimized ceiling of **216.437 M msg/s** median (3×10B, all above 200M).
- Full-topic QoS 0 end-to-end: **33.415 M msg/s** in the fresh 500M regression. Topic Alias end-to-end: **35.184 M msg/s** median in the sustained 2.1.2.2 release gate; 2.1.2.3 reached the controlled ~38–39 M/s tuning range with a best short run of **38.981 M/s**.
- Hybrid PQC is operational.
- Broker RAM is approximately 100–150 MiB in current QoS 0 gates.

---

# 2.1.3.0 — MQTT 5 completion / compliance hardening

Objective: reduce remaining protocol debt before adding product features.

## Main work

- Enhanced AUTH state machine.
- Authentication Method / Authentication Data.
- AUTH packet and associated reason codes.
- Request Problem Information.
- Request Response Information.
- Response Information where applicable.
- Server Reference / redirect semantics that we decide to support.
- Review of all CONNECT/CONNACK singleton properties.
- Review of reason codes by packet family.
- MQTT codec fuzzing.
- Malformed-packet corpus.
- Shared-subscription edge cases.
- Exhaustive MQTT UTF-8 corpus.
- Exhaustive Packet Identifier lifecycle tests.
- Disconnect/error matrix for protocol violations.

## Gates

- Expanded raw MQTT suite.
- Fuzzing with no crashes or panics.
- Reproducible malformed corpus.
- No regression in the 20M ingest / 2M end-to-end gates beyond normal variance.
- Do not declare “MQTT v5 100% compliant” until a broader external interoperability suite has been validated.

---

# 2.2.0.0 — Management REST API

Objective: turn Pipistrelle from a broker process into an administrable service.

## Planned API

- Broker status.
- Client list.
- Client by ClientID.
- Active connections.
- Online/offline Sessions.
- Subscriptions.
- Retained messages.
- Pending Wills.
- In-flight QoS state.
- Slow consumers.
- Bridge state.
- Summary metrics.
- Remote client disconnect.
- Session deletion.
- Retained-message deletion.
- Effective configuration inspection.

## Security

- API separate from the MQTT listener.
- Secure bind by default.
- Administration authentication.
- Basic RBAC for destructive operations.
- Audit log for administrative changes.

## Gates

- Versioned API at /api/v1.
- No destructive operation without authentication.
- Concurrency tests with clients connecting/disconnecting while the API is queried.
- The API must not hold long global locks that degrade routing.

---

# 2.3.0.0 — Pipistrelle Control Center

Objective: provide an operational web interface on top of the Management API.

## Main views

- Overview.
- Current throughput.
- CPU / RAM.
- Connections.
- Sessions.
- Subscriptions.
- Retained messages.
- In-flight QoS.
- Wills.
- TLS / PQC handshakes.
- Slow consumers.
- Bridge status.

## Individual client view

- ClientID.
- User/principal.
- IP.
- TLS / cipher / key exchange.
- Session Expiry.
- Subscriptions.
- Queued messages.
- In-flight QoS.
- Last Will.
- Disconnect.
- Delete Session.

## Gates

- The UI uses only the public/stable API.
- No critical logic lives only in the frontend.
- Works well on desktop and mobile.
- Does not expose configuration secrets.

---

# 2.4.0.0 — Persistence Engine v2

Objective: stop treating one SQLite operation per message as the final solution for heavy QoS paths.

This stage is especially important for improving QoS1/QoS2 without relaxing compliance.

## Target architecture

```text
MQTT state changes
        ↓
Persistence API
        ↓
WAL / journal
        ↓
batch writer
        ↓
storage backend
```

## Main work

- Persistence abstraction layer.
- Custom WAL or append-oriented journal.
- Commit batching.
- Group commit.
- Fewer fsyncs/transactions per message.
- Ordered persistent queue.
- Deterministic recovery.
- Compaction / cleanup.
- Storage-latency metrics.
- Queue-depth metrics.
- Storage backpressure.
- Compatibility/migration from existing SQLite state.

## Gates

- Crash recovery with SIGKILL.
- Zero loss of state already acknowledged as durable.
- Clear QoS1/QoS2 improvement over 2.1.2.0.
- Measurable and documented recovery of millions of records.
- No corruption after power-loss-style restart tests.

---

# 2.5.0.0 — Advanced Security / Enterprise Auth

Objective: evolve from solid local auth/ACLs into an identity platform suitable for enterprise integration.

## Planned backends

- File/local credentials.
- SQL auth backend.
- JWT.
- OAuth2/OIDC validation.
- mTLS certificate identity.
- LDAP/Directory integration if it provides real value.
- Custom backend through an extension/WASM API when available.

## Authorization

- Roles.
- Topic policies.
- Variables such as clientId and principal.
- Allowed QoS levels.
- Retain allowed/denied.
- Shared-subscription allowed/denied.
- Per-principal limits.

## Protection

- Connection-rate limits.
- Per-client publish-rate limits.
- Subscription-rate limits.
- Authentication brute-force protection.
- Certificate-revocation strategy.
- Audit log.

## Gates

- Fail closed.
- Credential rotation without restart where practical.
- Privilege-escalation tests.
- ACL/session ownership must never cross principals.

---

# 2.6.0.0 — Backup / Restore / Disaster Recovery

Objective: allow an operator to protect and rebuild a production broker.

## Target CLI

```text
pipistrelle backup create
pipistrelle backup inspect
pipistrelle backup verify
pipistrelle backup restore
```

## Included state

- Sessions.
- Subscriptions.
- Retained messages.
- Wills.
- In-flight QoS state.
- Metadata required by the persistence engine.

## Additional work

- Consistent backup while the broker remains operational.
- Checksums.
- Versioned format.
- Restore dry run.
- Optional backup encryption.
- Retention policies.

## Gates

- Byte/semantic-equivalent state after restore.
- Restore onto a new machine.
- Disaster test: delete the data directory, restore, then resume client Sessions.

---

# 2.7.0.0 — Advanced observability and diagnostics

Objective: make Pipistrelle easy to operate under load and easy to debug.

## Main work

- OpenTelemetry.
- Structured tracing.
- Temporary per-client tracing.
- Optional/sampled per-topic statistics.
- Storage-latency histograms.
- Routing latency.
- Authentication latency.
- Queue depth.
- Slow-consumer explorer.
- Session churn.
- QoS handshake latency.
- Bridge latency/reconnect state.
- Diagnostic bundle.

## Tooling examples

```text
pipistrelle trace client sensor-921
pipistrelle diagnose create
```

## Gates

- Tracing disabled means negligible impact.
- Configurable sampling.
- Diagnostic bundles contain no secrets.
- Metrics are documented and stable.

---

# 2.8.0.0 — Extension System / WASM

Objective: build an extension ecosystem without allowing arbitrary plugins to compromise the broker.

## Preferred direction

**Sandboxed WASM extensions.**

## Initial hooks

```text
on_connect
on_authenticate
on_authorize
on_publish
on_subscribe
on_unsubscribe
on_disconnect
on_will
```

## Requirements

- Memory limit per plugin.
- CPU/fuel limit.
- Timeout.
- No filesystem/network access by default.
- Capability-based permissions.
- Hot reload where safe.
- Versioned SDK.
- A plugin crash must not bring down Pipistrelle.

## Gates

- An infinite plugin cannot block routing.
- A plugin panic/trap cannot bring down the broker.
- Hooks can be disabled without appreciable overhead.
- ABI/SDK is versioned.

---

# 2.9.0.0 — Experimental clustering / Beta

Objective: build the first distributed Pipistrelle. **This is not stable HA or 3.0 yet.**

## Phase 1 — Membership

- Node identity.
- Discovery.
- Membership.
- Heartbeats.
- Node join/leave.
- Failure detection.

## Phase 2 — Distributed routing

```text
Publisher → Node A
Subscriber → Node C
            ↓
        message arrives
```

- Topic routing between nodes.
- Subscription propagation.
- Initial shared-subscription ownership.

## Phase 3 — Session ownership

- ClientID owner node.
- Cross-node takeover.
- Session location lookup.
- Reconnect to a different node.

## Phase 4 — Replicated state

- Retained messages.
- Sessions.
- Subscriptions.
- Offline queues.
- QoS1/2 state.
- Wills.
- Session Expiry.

## Expected 2.9 state

- Experimental/beta.
- Documented limitations.
- No zero-downtime promise yet.
- Do not call the cluster “production HA” until it passes the 3.0 gates.

---

# 3.0.0.0 — Stable clustering + High Availability

3.0.0.0 exists only when the cluster is no longer experimental.

This is the product's change in meaning:

```text
Pipistrelle 2.x
standalone MQTT broker

        ↓

Pipistrelle 3.x
distributed MQTT platform
```

## Target architecture

```text
                 Load Balancer
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
  Pipistrelle A  Pipistrelle B  Pipistrelle C
        │             │             │
        └──────── cluster bus ───────┘
                      │
              replicated state
```

## Requirements for calling it 3.0

### High availability

- Failure of one node does not stop the complete service.
- Clients can reconnect to another node.
- Session state is recoverable from another node.
- Retained messages remain available.
- Wills are handled correctly during node failure.
- QoS1/QoS2 state is not corrupted by failover.

### Distributed routing

- Publisher and subscriber can be on different nodes.
- Wildcards work across nodes.
- Shared subscriptions have well-defined ownership/fairness.
- No routing loops.

### State

- Replication consistency is explicitly documented.
- No silent split brain.
- Recovery after partition.
- Node rejoin.
- Rebalancing.

### Operations

- Rolling restart.
- Rolling upgrade.
- Node drain.
- Cluster health API.
- Replication-lag metrics.
- Cluster-aware backup/restore.

### Platform

- Cluster-aware Management API.
- Cluster-aware Control Center.
- Consistent security/policies on every node.
- Clear extension behavior in a cluster.

## Definitive 3.0 gate

Minimum mandatory test:

```text
Subscriber → Node C
Publisher  → Node A

A→C message delivery works

SIGKILL Node A

cluster remains operational
publisher reconnects to Node B
subscriber keeps its Session
retained messages remain available
QoS state continues correctly

Node A returns
rejoin + sync
no split brain
```

Also required:

- Prolonged multi-node stress.
- Network-partition tests.
- Packet loss/reordering on the cluster bus.
- Node crash loops.
- Persistence recovery.
- Upgrade from 2.9.x.
- Documented distributed benchmark.

Only after these gates may 3.0.0.0 be published.

---

# What we must NOT do too early

- Do not call a cluster that merely connects two nodes “3.0”.
- Do not add a UI before the Management API is stable.
- Do not add complex Data Policies before persistence, observability and cluster foundations.
- Do not sacrifice MQTT correctness for benchmarks.
- Do not replace bounded queues with unbounded queues to increase throughput.
- Do not hide QoS1/QoS2 regressions behind QoS0 numbers.
- Do not claim “MQTT v5 100% compliant” without sufficient external validation.

---

# Later or parallel ideas that do not block 3.0

These can be introduced when their corresponding foundation is mature, but they must not distract from the main sequence:

## Pipistrelle Edge AI — optional edge-inference specialization

Objective: use local NPU accelerators to turn selected MQTT flows into inference pipelines without contaminating the main broker hot path.

The product separation must remain explicit:

```text
Pipistrelle Core
pure MQTT broker / maximum performance

        + optional

Pipistrelle Edge AI
MQTT + accelerated local inference rules
```

The first implementation should prefer an **isolated sidecar or extension** over direct dataplane integration. Inference must never run inline in critical routing if it can block MQTT clients.

### Target flow

```text
MQTT PUBLISH
     ↓
topic/rule match
     ↓
bounded inference queue
     ↓
NPU / accelerator backend
     ↓
local model
     ↓
result published back through MQTT
```

### Main work

- NPU backend for Orange Pi 6 / CIX when the driver/runtime is stable in our Linux stack.
- Accelerator-backend abstraction to avoid coupling Pipistrelle to one vendor.
- topic → model → output-topic rules.
- Local model registry and safe hot reload.
- Optional batching by model.
- Inference timeouts.
- Bounded queues and backpressure separate from normal MQTT outbound queues.
- Explicit optional CPU fallback.
- Metrics for inferences/s, p50/p95/p99 latency, queue depth, timeouts and failures.

### Initial use cases

- Camera → object detection → detections topic.
- Audio → event classification → alerts topic.
- Industrial telemetry → anomaly detection → incident topic.
- Sensors → local classification/prediction → derived topic.
- Compatible payloads → embeddings/classification → MQTT result.

### Preferred integration

After 2.8.0.0, Edge AI should be able to live as an isolated extension/capability: on_publish selects the work, inference runs outside the broker's critical executor and the result returns through a stable internal API.

### Mandatory gates

- Edge AI disabled means negligible impact on Pipistrelle Core.
- A failing model/NPU/runtime cannot bring down the broker.
- No inference queue may be unbounded.
- A slow model cannot block MQTT routing for unrelated clients.
- Overload/backpressure policy is explicit and tested.
- Core and Edge AI benchmarks are reported separately; TOPS must not be mixed with MQTT msg/s.
- Prolonged soak tests with active inference are required before production readiness.
- Validate at least one real end-to-end pipeline: PUBLISH → NPU inference → result PUBLISH.

This line does not replace Pipistrelle Core. It is a specialization for edge deployments where MQTT networking and local inference share one node.

Additional possible work:

- JSON Schema validation.
- Protobuf schema validation.
- Data Policies / policy engine.
- Dead-letter routing.
- Rule engine.
- MQTT ↔ Kafka/NATS connectors.
- Cloud bridge packs.
- Helm chart.
- Kubernetes Operator.
- Multi-architecture release automation.
- Optional post-quantum signatures when practical interoperability is available.

---

# Version summary

| Version | Primary objective |
|---|---|
| 2.1.2.0 | Solid MQTT flow control/compliance |
| 2.1.2.1 | Inbound Topic Alias + >200M/s optimized ingest ceiling |
| 2.1.2.2 | Bidirectional Topic Alias + ~35M/s E2E fast route |
| 2.1.2.3 | Current baseline: SVE Topic Alias E2E + CI/security hardening (~38–39M/s tuning) |
| 2.1.3.0 | Remaining MQTT 5 completion + fuzzing/compliance |
| 2.2.0.0 | Management REST API |
| 2.3.0.0 | Control Center / Web UI |
| 2.4.0.0 | Persistence Engine v2 / WAL / batching |
| 2.5.0.0 | Advanced security / enterprise auth / quotas |
| 2.6.0.0 | Backup, restore and disaster recovery |
| 2.7.0.0 | Observability, tracing and diagnostics |
| 2.8.0.0 | Sandboxed WASM extensions |
| 2.9.0.0 | Experimental / beta clustering |
| 3.0.0.0 | Stable clustering + High Availability |
| Edge AI | Parallel specialization: MQTT + accelerated local NPU inference without contaminating Core |

---

This roadmap is deliberately sequential. Versions may receive intermediate builds/patches such as 2.4.0.1 or 2.4.0.2 without consuming a new minor version while a stage is still being hardened.
