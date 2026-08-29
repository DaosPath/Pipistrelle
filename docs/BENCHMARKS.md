# Pipistrelle V2 Benchmarks

## ARM64 reference platform

Baseline collected on 2026-08-25 using an Orange Pi running Linux `6.18.43-current-arm64`.

- Architecture: AArch64
- CPU: 12 cores, ARM Cortex-A520 + Cortex-A720, up to 2.6 GHz on the A720 cluster
- RAM: 15 GiB usable
- Broker: Pipistrelle V2 `2.0.0.0`, release build, Docker container
- Load generator: `target/release/pipistrelle-bench`, native AArch64 ELF executed directly on the host
- Transport path: host loopback (`127.0.0.1`) to Docker-published broker ports
- Payload: 128 bytes unless stated otherwise

The Python/Paho suite remains an integration test. It is not used to establish maximum broker throughput because it became the bottleneck far before the broker did.

## Native benchmark semantics

The native generator speaks MQTT v5 directly over Tokio sockets and rustls. It does not use Paho, Python, or an external MQTT client library.

`loopback` creates one private subscription per client and counts a message only after it has traversed the broker and returned to the client. This exercises decode, authorization, topic routing, serialization and outbound network delivery.

`ingest` publishes without a subscriber. QoS 0 has no protocol acknowledgement, so every worker appends a QoS 1 marker after its final QoS 0 PUBLISH. TCP ordering means the marker PUBACK proves all preceding PUBLISH packets on that connection were processed by the broker. Prometheus counter deltas were also checked independently.

Reported MiB/s is payload throughput only; MQTT/TCP/TLS framing overhead is not included.

## V2 2.0.0.0 baseline

| Scenario | Clients | Messages | Result |
|---|---:|---:|---:|
| TCP ingest QoS 0 | 1 | 1,000,000 | **1.918 M msg/s** |
| TCP ingest QoS 0 | 10 | 1,000,000 total | **3.308 M msg/s** |
| TCP ingest QoS 0 | 50 | 1,000,000 total | **3.351 M msg/s** |
| TCP loopback QoS 0 | 1 | 100,000 | **273.9 k msg/s** |
| TCP loopback QoS 0 | 10 | 1,000,000 total | **854.7 k msg/s** |
| TCP loopback QoS 0 | 50 | 1,000,000 total | **673.5 k msg/s** |
| TCP ingest QoS 1 | 10 | 200,000 total | **377.0 k msg/s** |
| TCP loopback QoS 1 | 1 | 10,000 | **25.3 k msg/s** |
| TCP loopback QoS 1 | 10 | 50,000 total | **124.4 k msg/s** |
| TLS hybrid loopback QoS 0 | 10 | 200,000 total | **638.4 k msg/s** |
| TLS classical loopback QoS 0 | 10 | 200,000 total | **672.1 k msg/s** |

The hybrid TLS run negotiated `X25519MLKEM768` on all 10 clients. The classical run negotiated `X25519` on all 10 clients.

### Sustained ingest

A longer 50-client run sent **50,000,000 QoS 0 messages** in **14.383 s**, or **3.476 M msg/s sustained** and **424.4 MiB/s of payload**. The Prometheus counter increased by exactly `50,000,050`: 50 million payload messages plus one QoS 1 completion marker per client.

During this run the broker sampled around 9–10.4 CPU cores worth of utilization, used roughly 160 MiB of RAM, and the hottest reported thermal zone moved from about 44 °C to 46 °C.

### Sustained end-to-end routing

A 10-client loopback run delivered **10,000,000 messages** in **10.898 s**, or **917.6 k msg/s sustained**. The Prometheus publish counter increased by exactly 10 million.

This test exposed a memory/backpressure issue: broker RSS rose past 1 GiB while outbound queues were under sustained pressure. Pipistrelle currently uses an unbounded Tokio channel per client writer. That architecture can accumulate a large queue when routing produces messages faster than a socket drains. This is a V2 optimization priority; the result should not be interpreted as a safe production capacity limit until bounded backpressure is implemented and profiled.

## TLS interpretation

The post-quantum component affects TLS key establishment. Once a TLS 1.3 session is established, bulk record encryption uses the negotiated symmetric cipher, so steady-state hybrid-vs-classical message throughput should be close. Handshake latency and connection-rate benchmarks are the more meaningful place to quantify ML-KEM overhead.

## Reproducing

Run the normal native matrix:

```bash
./scripts/bench-native-orange-pi.sh
```

Include the sustained 50M ingest and 10M loopback cases:

```bash
FULL=1 ./scripts/bench-native-orange-pi.sh
```

Results are written under `bench-results/`, which is ignored by Git.

## V2 2.0.0.1 backpressure validation

`2.0.0.1` replaces the unbounded per-client writer channel with a bounded queue (`PIPISTRELLE_CLIENT_QUEUE_CAPACITY`, default `1024`) and propagates queue pressure back to publishers. No intentional packet dropping is used.

### Direct before/after

For the release gate, both versions were started in fresh containers on the same Orange Pi and driven by the same native Rust load generator with the same 10-client / 10-million-message workload.

| Scenario | 2.0.0.0 | 2.0.0.1 | Change |
|---|---:|---:|---:|
| Sustained TCP loopback, 10 clients, 10M QoS 0 | **928.8 k msg/s** | **900.3 k msg/s** | **-3.08%** |
| Observed broker memory peak | **1.501 GiB** | **146.3 MiB** | **~90.5% lower** |
| Broker memory ~2 s after the run | **1.055 GiB** | **123.1 MiB** | **~88.6% lower** |
| TCP loopback, 50 clients, 1M QoS 0 | 673.5 k msg/s | **758.7 k msg/s** | **+12.7%** |
| TCP loopback, 10 clients, 50k QoS 1 | 124.4 k msg/s | **130.3 k msg/s** | **+4.7%** |

The controlled `2.0.0.1` run delivered all 10,000,000 loopback messages successfully. The bounded queues reached capacity **133,705** times and accumulated **65.441 seconds** of producer wait across concurrent tasks. This cumulative wait can exceed wall-clock time because multiple publishers may be waiting concurrently.

The release trade-off is therefore explicit: about **3% lower throughput** in this sustained 10-client stress case in exchange for roughly a **10x reduction in observed peak broker memory** and elimination of unbounded per-client output growth. No messages were intentionally dropped.

### Ingest ceiling

The per-client outbound queue is not exercised by the `ingest` benchmark because there are no subscribers. Two sustained 50-million-message runs on `2.0.0.1` measured **3.288 M msg/s** and **3.403 M msg/s**, versus **3.476 M msg/s** for the earlier `2.0.0.0` reference run. This represents roughly 2–5% run-to-run/async-path overhead in the current measurements and should be profiled further rather than treated as a queue-backpressure cost.

### TLS/PQC regression check

A native 10-client TLS loopback run negotiated `X25519MLKEM768` on all 10 connections and completed 200,000 messages at **706.9 k msg/s** with zero failures. The cryptographic mode therefore remains operational after the backpressure refactor. Short TLS throughput runs show substantial run-to-run variance on the shared Orange Pi and are not used as a release gate.

## V2 2.0.0.2 isolation / observability validation

`2.0.0.2` keeps the bounded client queues from `2.0.0.1` and adds policy/bridge isolation plus sampled latency telemetry. Benchmarks below were run on the same Orange Pi with the native Rust generator. Because this SBC also runs other services, short-run throughput can vary materially; release conclusions use clean-container runs and treat later hot-system runs as variance rather than exact regressions.

### Clean 10M routing gate

A fresh-container run delivered **10,000,000 QoS 0 loopback messages** with 10 clients at **899.7 k msg/s**. Broker memory stayed around **105–106 MiB** during the run and about **104 MiB** after the run. This is effectively equal to the controlled `2.0.0.1` release gate of **900.3 k msg/s** while adding the new policies and telemetry.

With the default latency sample rate of `64`, the 10M run produced exactly **156,250 latency samples**. The sampled histogram reported approximately:

- p50: **<= 5 µs**
- p95: **<= 5 µs**
- p99: **<= 250 µs**

These are bucket upper bounds for internal publish-routing time, including queue waits when sampled; they are not client-observed end-to-end network latency.

### Sustained ingest

Two 50-million-message sustained ingest runs during `2.0.0.2` development measured roughly **3.03–3.08 M msg/s** (about **370–377 MiB/s** of 128-byte payload). The ingest path is extremely sensitive to CPU scheduling and other workloads on the shared Orange Pi, so this range is recorded as an observed result rather than a hard capacity claim.

### TLS hybrid PQC

A native 10-client TLS 1.3 loopback check delivered **200,000 messages at 732.3 k msg/s** and all 10 connections negotiated **`X25519MLKEM768`**.

### Policy validation

The new policies were exercised against dedicated temporary broker instances:

- Subscription quota set to 2: the first two subscriptions were granted and the third returned MQTT v5 reason **`0x97`**; `pipistrelle_subscription_quota_rejections_total` increased to 1.
- Remote bridge unavailable, queue capacity 16, `drop-newest`: after 100 matching `sensor/` publishes, the queue held **16**, and exactly **84** additional messages were counted in both bridge-full and bridge-drop counters.
- Slow consumer with queue capacity 16, `disconnect`, 100 ms timeout: a raw MQTT subscriber stopped reading from its socket; the broker recorded **84** queue-pressure events, disconnected that client exactly once, removed the active connection, and the publisher completed successfully.

## V2 2.0.0.3 — 20M/s ingest milestone

The `2.0.0.3` optimization target was **20 M msg/s** for native TCP QoS 0 ingest. This is an **ingest ceiling** measurement: there are no subscribers and no remote bridge, so it measures MQTT decode/auth/accounting/intake rather than fan-out delivery.

### Optimization progression

Using the same 50-client, 128-byte, QoS 0 native Rust workload on the Orange Pi:

| Step | Sustained result |
|---|---:|
| V2.0.0.2 pre-hot-path range | ~3.0–3.4 M msg/s |
| Allocation-free ACL + zero-route router bypass | **13.32 M msg/s** |
| Cached global ACL permissions | **13.58 M msg/s** |
| Per-session publish counters | **18.91 M msg/s** |
| Synchronous zero-routing QoS 0 decode fast path | **21.31 M msg/s** |
| After integration + exact route-count recovery | **21.68 M msg/s** |

### 200-million-message sustained validation

A clean run sent **200,000,000 QoS 0 messages** using 50 clients and 128-byte payloads in **9.318 s**:

- **21.463 M msg/s sustained**
- **2,620 MiB/s payload throughput**
- Prometheus delta: exactly **200,000,050** (200M messages + 50 QoS 1 completion markers)
- **0 benchmark failures**
- Broker CPU samples: roughly **4.9–6.3 CPU cores**
- Native generator: roughly **1.8–2.1 CPU cores**
- Broker memory observed during the run: roughly **98–121 MiB**
- Broker memory after the run: roughly **80 MiB**

The result therefore exceeded the 20 M msg/s target over a sustained 200M-message run rather than only a short burst.

### Full-routing regression gate

The same build delivered **10,000,000 QoS 0 loopback messages** with 10 clients at **906.9 k msg/s**. Broker memory was about **104 MiB** after the run, so the ingest optimization did not undo bounded-memory routing.

### Hybrid PQC regression gate

A 10-client TLS 1.3 loopback test delivered **200,000 messages at 634.2 k msg/s**, with all 10 connections negotiating **`X25519MLKEM768`** and zero failures.

### Interpretation

The **20+ M msg/s** figure must not be interpreted as 20M end-to-end delivered messages/s. It is the current single-node **QoS 0 intake ceiling** for the optimized zero-routing case. Fan-out/routing, QoS 1 persistence, WebSockets, and TLS have different ceilings and must be benchmarked independently.

## V2 2.0.0.4 — 2M/s sustained end-to-end milestone

The target for `2.0.0.4` was at least **2.0 M msg/s end-to-end** for native TCP MQTT v5 QoS 0 routing while preserving bounded queues and normal broker semantics. The workload uses 10 clients, each subscribed to its own exact topic and publishing 128-byte payloads to that topic.

### Separating broker gains from benchmark gains

The native benchmark reader itself had become a bottleneck: it previously performed multiple `read_exact` operations and allocated a body `Vec` for every received MQTT packet. After converting it to a buffered multi-packet parser, the published `v2.0.0.3` tag was re-tested unchanged with the new generator and reached **1.321 M msg/s** over 10 million messages.

The optimized `2.0.0.4` broker, driven by that same new generator on the same Orange Pi, reached **2.393 M msg/s** in a 10-million-message run. That is roughly **+81% broker throughput** versus `v2.0.0.3` under the corrected measurement path.

### Sustained 50-million-message release gate

The final `2.0.0.4` build delivered **50,000,000 QoS 0 messages** in **20.893 s**:

- **2.393 M msg/s sustained end-to-end**
- **292.1 MiB/s delivered payload throughput**
- Prometheus publish delta: exactly **50,000,000**
- **0 benchmark failures**
- Broker CPU samples: roughly **8.0–8.8 CPU cores** during most of the run
- Broker memory: roughly **103–105 MiB** during the sustained run
- Broker memory about two seconds after the run: roughly **102 MiB**
- Bounded-queue pressure was still active and observable (`52,663` pressure events; cumulative concurrent wait about `16.7 s`)

This is a true loopback routing test: every counted message entered the broker and was delivered back to a subscriber. It is not the `ingest` fast path used for the 20M/s milestone.

### A/B progression

| Change | 10-client QoS 0 loopback |
|---|---:|
| `v2.0.0.3`, corrected buffered benchmark reader | **1.321 M msg/s** |
| Direct QoS 0 encoder | ~**1.0 M/s** with the older reader |
| Exact-topic routing cache + writer batching, older reader | **1.69–1.74 M/s** |
| Final broker + buffered native reader | **2.31–2.39 M/s** |
| Final sustained 50M gate | **2.393 M msg/s** |

The older-reader intermediate numbers are diagnostic only; the A/B comparison against `v2.0.0.3` uses the same corrected generator on both broker versions.

### QoS 1 and hybrid PQC regression gates

The final build also completed a 10-client, 200,000-message QoS 1 loopback run at **427.2 k msg/s** with zero failures. A clean TLS 1.3 hybrid run delivered 200,000 QoS 0 messages at **2.108 M msg/s** in a short run, with all **10/10** connections negotiating `X25519MLKEM768` and setup p50 around **200 ms**. The TLS number is a short regression check, not the sustained release headline.

## V2 2.1.0.0 — protocol-feature regression gate

`2.1.0.0` adds QoS 2, retained messages, Last Will, persistent-session/takeover state and principal-bound Session ownership. The release keeps a QoS 0 performance regression gate so protocol completeness does not silently undo the `2.0.0.4` routing work.

Final Orange Pi release gates (ARM64, 128-byte payload, native Rust load generator):

- **50M end-to-end QoS0:** 2.835 M msg/s sustained in 17.634 s on a fresh container; broker memory stayed about 124–125 MiB during the run and 121.6 MiB after 2 s idle.
- **50M ingest QoS0:** 21.485 M msg/s with the Prometheus publish counter increasing by exactly 50,000,050 (50M publishes + 50 QoS1 completion markers).
- **Final 10M end-to-end smoke after principal-binding hardening:** 2.941 M msg/s, 0 failures, about 103.2 MiB broker memory after the run.
- **Hybrid PQC TLS smoke:** 200k end-to-end messages at 1.667 M msg/s, 0 failures, with `X25519MLKEM768` negotiated on 10/10 connections.

These are regression gates, not a claim that protocol-heavy QoS1/QoS2 paths share the QoS0 ceiling.

## V2 2.1.1.0 — properties/crash-persistence regression gate

`2.1.1.0` carries MQTT v5 Application Message properties through live routing, retained/offline QoS state and Last Will recovery. The extra metadata path is bypassed by the zero-route QoS0 fast path when no forwarding or persistence is needed.

Final Orange Pi candidate gates, TCP, 128-byte payload:

- **50M ingest QoS0:** 20.134 M msg/s, 0 failures.
- **50M end-to-end QoS0:** 2.709 M msg/s sustained, 0 failures, ~122.4 MiB broker memory after the run.

An earlier compliance-first candidate measured 17.2–19.5 M/s ingest because it scanned all PUBLISH property fields and used a Unicode wildcard search in the zero-route path. The final implementation keeps the same protocol validation while reducing that path to the fields which can require protocol handling and an allocation-free byte wildcard check.

## V2 2.1.2.0 — flow-control compliance regression gate

`2.1.2.0` adds bilateral Receive Maximum handling, Maximum Packet Size enforcement, durable UNSUBSCRIBE, server-assigned ClientIDs, incremental CONNECT framing and stricter wire validation. QoS0 paths do not allocate flow-control state.

Final exact-image Orange Pi gates, native Rust load generator, 128-byte payload:

- **50M QoS0 ingest, 3 fresh-container repeats:** 20.792 / 20.328 / 18.582 M msg/s; **median 20.328 M msg/s**, 0 failures. The run-to-run spread is retained in the report instead of cherry-picking the maximum.
- An exact 50M ingest counter gate increased `pipistrelle_messages_published_total` by **50,000,050**, matching 50M QoS0 publishes plus 50 QoS1 completion markers.
- **50M QoS0 end-to-end:** **2.656 M msg/s**, 0 failures, ~**103.5 MiB** broker memory after the run.
- **200k QoS1 end-to-end:** **210.97k msg/s**, 0 failures. The stricter bilateral flow-control path adds synchronization and the existing SQLite-per-message persistence remains the main bottleneck; this is tracked as a performance target rather than relaxed for speed.
- **200k hybrid PQC TLS QoS0:** **2.112 M msg/s**, 0 failures, `X25519MLKEM768` on **10/10** connections.

During the ingest repetitions the board was ~41–45 °C with a shared-system load average around 4, which explains part of the observed scheduling variance.

## V2 2.1.2.3 — SVE Topic Alias E2E closing pass

`2.1.2.3` is the final micro-optimization pass before moving on to the next product work. On Linux/AArch64 with SVE available, the 9-byte Topic Alias QoS0 structural scanner gathers four frames per vector group and processes 16-frame blocks. Runtime feature detection preserves portability, and any mismatching block falls back to the scalar matcher to identify the exact first changed frame before general MQTT parsing.

The native QoS0 loopback benchmark also keeps its `WriteHalf` directly owned by the publisher task because QoS0 has no reader-side PUBACK writes. This removes one Tokio mutex from the load generator only; the receiver still validates and counts the complete routed stream.

Controlled Orange Pi tuning reached a best short validated run of **38.981 M msg/s** and repeatedly operated in the **~38–39 M msg/s** region. A sustained 40 M/s claim is intentionally **not** made. The `2.1.2.2` sustained ~2B release median of 35.184 M/s remains the prior release baseline, and the >200M numbers below remain a separate ingest/sendfile ceiling rather than an E2E result.

Release engineering in `2.1.2.3` also adds reproducible CI, RustSec/CodeQL security checks and a pinned Rust 1.98.0 toolchain.

### 1T end-to-end endurance validation

The final `2.1.2.3` Orange Pi validation ran two independent one-trillion-message endurance cases. This is a long-duration correctness and stability check, not a production capacity or SLA claim.

Common workload:

- **Target:** `1,000,000,000,000` MQTT v5 PUBLISH messages per run.
- **Transport:** TCP loopback, native Rust/Tokio generator, no Python/Paho.
- **Routing:** end-to-end loopback; each client publishes to a topic and receives the routed message back.
- **Clients:** 16; `62,500,000,000` messages per client.
- **Payload:** 128 bytes; QoS 0; Topic Alias enabled with `accept-topic-alias=1`.
- **Order:** fresh `window=1280` run followed by a fresh `window=1024` run on the same Orange Pi.
- **Accounting:** Prometheus `pipistrelle_messages_published_total` delta had to equal exactly 1T; the benchmark result also required `failures=0`.

| Window | Duration | Throughput | Payload throughput | Prometheus delta | Failures | Peak RSS | Peak temp |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1280 | 26,992.242 s (7 h 29 min 52 s) | **37.048 M msg/s** | **4,522.422 MiB/s** | **1,000,000,000,000** | **0** | 191.9 MiB | 55 °C |
| 1024 | 28,731.509 s (7 h 58 min 52 s) | **34.805 M msg/s** | **4,248.656 MiB/s** | **1,000,000,000,000** | **0** | 172.8 MiB | 57 °C |

Both runs finished with `status=ok` and `bench_exit=0`. The release runner also returned `window_1280_runner_rc=0` and `window_1024_runner_rc=0`. The full local evidence is retained under `bench-results/v2.1.2.3/1t-hardened-series-20260828-040803-1cb2127/` (ignored by Git), including the exact-image hashes, start/end markers, Prometheus snapshots, telemetry and machine-readable summaries.

These 1T results should not be compared directly with the short `38–39 M msg/s` tuning result, the earlier ~2B-message release baseline, or the separate ingest/`--sendfile` ceilings. They exercise the end-to-end routing path for many hours and are reported separately to keep workload semantics visible.

## V2 2.1.2.2 — Topic Alias end-to-end routing

`2.1.2.2` adds Server→Client Topic Alias negotiation and a specialized exact-route fast path while preserving connection-local alias semantics. The receiving client must advertise a non-zero Topic Alias Maximum in CONNECT. The broker sends the first mapping as full Topic Name + alias and may then send zero-length Topic Name + alias. Publisher-side alias remaps invalidate the fast-route cache through a connection-local epoch.

Sustained ~2B-message end-to-end runs on the Orange Pi:

| Run | Throughput | Approx. elapsed | Failures |
|---|---:|---:|---:|
| 1 | **35.184 M msg/s** | ~56–58 s | 0 |
| 2 | **34.543 M msg/s** | ~56–58 s | 0 |
| 3 | **35.587 M msg/s** | ~56–58 s | 0 |

**Median: 35.184 M msg/s.** Prometheus accounting was exact in every run. The previous Topic Alias end-to-end implementation measured **2.469 M msg/s**, making this about a **14.25×** improvement.

Fresh regression gates on the same source keep benchmark categories separate:

- **500M full-topic QoS0 end-to-end:** **33.415 M msg/s**, 0 failures, exact counter accounting.
- **500M Topic Alias QoS0 ingest with `--sendfile`:** **150.808 M msg/s**, 0 failures, exact counter accounting.
- Functional release gates: **51/51 Rust**, **25/25 raw MQTT v5**, **10/10 SIGKILL/restart**, **6/6 integration**.

The profile of the final Topic Alias E2E candidate is dominated by kernel copy/VM work (~30.07% `__arch_copy_to_user`, ~9.82% `__arch_copy_from_user`, ~5.86% page clear); the alias scanner is ~2.81% and outbound queue work ~0.56%. This is why additional routing-side hashing/micro-optimization is no longer the main bottleneck.

Do not compare these E2E figures directly with the `2.1.2.1` 10B ingest ceiling: ingest and subscriber delivery exercise different datapaths.

## V2 2.1.2.1 — Topic Alias ingest: 161M normal-host median / 211M optimized ceiling

`2.1.2.1` keeps the MQTT correctness/flow-control behavior of `2.1.2.0`, adds Client→Server Topic Alias, a zero-route ARM64 fast path, and a Linux-only native benchmark backend `--sendfile`. Results below use 128-byte payloads and ordered QoS1 completion markers. Docker remains supported but is not used for native ceiling figures.

### Official sustained V2 gate — 10 billion PUBLISH per run

One-billion-message runs complete in only a few seconds at the current throughput, so V2 uses 10B as the sustained performance gate and retains 1B for tuning/short regressions.

Normal host/network configuration, 13 clients / window 4096:

| Run | Workload PUBLISH | Throughput | Elapsed | Failures | Prometheus delta | Peak RSS | Peak temp |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9,999,999,997 | **163.550 M/s** | 61.14 s | 0 | **10,000,000,010** | 44.6 MiB | 43 °C |
| 2 | 9,999,999,997 | **160.064 M/s** | 62.48 s | 0 | **10,000,000,010** | 42.6 MiB | 44 °C |
| 3 | 9,999,999,997 | **164.866 M/s** | 60.66 s | 0 | **10,000,000,010** | 43.5 MiB | 44 °C |

**Sustained normal-host median: 163.550 M msg/s; minimum: 160.064 M msg/s.** No thermal or memory degradation appeared across the minute-long runs.

Optimized ceiling, clean network namespace + advertised hardware max frequencies, 14 clients / window 1792:

| Run | Workload PUBLISH | Throughput | Elapsed | Failures | Prometheus delta | Peak RSS | Peak temp |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9,999,999,996 | **220.274 M/s** | 45.40 s | 0 | **10,000,000,010** | 44.3 MiB | 45 °C |
| 2 | 9,999,999,996 | **216.351 M/s** | 46.22 s | 0 | **10,000,000,010** | 43.8 MiB | 45 °C |
| 3 | 9,999,999,996 | **216.437 M/s** | 46.20 s | 0 | **10,000,000,010** | 43.2 MiB | 45 °C |

**Sustained optimized median: 216.437 M msg/s; minimum: 216.351 M msg/s. All three 10B runs remained above 200 M/s.** CPU governor/min/max values were restored after every run. This is the primary evidence for the >200M optimized-ceiling claim.

Raw run JSON and a machine-readable summary live in `bench-results/v2.1.2.1/`.

### Short/tuning gate — normal host/network configuration

Command shape:

```bash
./target/release/pipistrelle-bench \
  --mode ingest --clients 13 --messages 76923076 \
  --payload 128 --qos 0 --window 4096 \
  --topic-alias --sendfile
```

Three fresh broker instances, original host network rules and original CPU frequency caps:

| Run | Workload PUBLISH | Throughput | Failures | Prometheus delta |
|---|---:|---:|---:|---:|
| 1 | 999,999,988 | **161.452 M msg/s** | 0 | **1,000,000,001** |
| 2 | 999,999,988 | **162.198 M msg/s** | 0 | **1,000,000,001** |
| 3 | 999,999,988 | **154.922 M msg/s** | 0 | **1,000,000,001** |

**Median: 161.452 M msg/s; minimum: 154.922 M msg/s.** The delta includes 13 ordered QoS1 completion markers per run.

### Optimized hardware/software ceiling

This is deliberately reported separately from the normal-host result. The benchmark ran inside a clean Linux network namespace with only loopback, so host Docker/nftables/conntrack rules did not process the traffic. CPU policy max/min values were temporarily set to each policy's advertised hardware maximum and the governor to `performance`; the exact original governor/min/max values were restored after every run. The broker/loadgen were pinned to CPUs `0,1,4,5,6,7,8,9,10,11`.

Topic Alias ingest, 14 clients / window 1792, three fresh ~1B-message brokers:

| Run | Workload PUBLISH | Throughput | Failures | Prometheus delta |
|---|---:|---:|---:|---:|
| 1 | 999,999,994 | **211.520 M msg/s** | 0 | **1,000,000,008** |
| 2 | 999,999,994 | **222.957 M msg/s** | 0 | **1,000,000,008** |
| 3 | 999,999,994 | **210.799 M msg/s** | 0 | **1,000,000,008** |

**Median: 211.520 M msg/s; minimum: 210.799 M msg/s; maximum: 222.957 M msg/s.** All three long runs exceeded 200 M PUBLISH/s. Temperature observed during the preceding full-frequency validation remained around 40–42 °C. These are ceiling conditions, not a promise for the default host configuration.

### Why `--sendfile` is a valid separate benchmark engine

The old generator repeatedly copied its prebuilt QoS0 window from load-generator userspace into the kernel. At ~80M/s that generator copy was already stealing a large fraction of the SoC from the broker. `rust-native-sendfile` keeps the MQTT stream identical but stores each publisher's prebuilt window in its own `memfd` and asks Linux to transmit it with `sendfile()`. CONNECT, CONNACK parsing, Topic Alias Maximum validation, the first mapping PUBLISH, final QoS1 marker and PUBACK requirement all remain explicit. The benchmark JSON includes `"sendfile": true` and `"engine": "rust-native-sendfile"` so results cannot be confused with the portable engine.

The broker itself still validates every steady alias frame. The AArch64 scalar9×16 matcher checks the 9 structural bytes of every frame and falls back to the general MQTT path on any mismatch. It intentionally ignores payload bytes; a dedicated unit test verifies that 16 different payloads are accepted while a changed alias stops the batch at the exact frame boundary.

### Fresh current-source regression gates

Each performance case below used a fresh native broker and fresh SQLite DB. `PIPISTRELLE_WRITER_BATCH_PACKETS=1024` was enabled for the routing regression set.

- **500M full-topic QoS0 ingest (28 clients, window 16384):** **57.987 M msg/s**, 0 failures, Prometheus delta **500,000,004** (499,999,976 QoS0 publishes + 28 completion markers).
- **~50M full-topic QoS0 end-to-end (13 clients, window 1024):** **33.203 M msg/s**, 0 failures, Prometheus delta exactly **49,999,989**.
- **200k QoS1 end-to-end:** **221.82k msg/s**, 0 failures, Prometheus delta exactly **200,000**.
- **200k hybrid PQC TLS QoS0:** data phase **7.458 M msg/s**, 0 failures, `X25519MLKEM768` on **10/10** connections, setup p50 **197.0 ms** / p95 **233.2 ms**; Prometheus delta **200,010** includes the 10 completion markers.
- Functional gates at the `2.1.2.1` release point: **45/45 Rust**, **24/24 raw MQTT v5**, **10/10 SIGKILL/restart**, **6/6 integration**.

Do not compare the 161/211 M/s Topic Alias figures directly to full-topic ingest as if the wire format were identical, and do not compare ingest to end-to-end routing. Topic Alias is a standard MQTT 5 optimization that removes repeated Topic Name bytes after a mapping is established; all benchmark categories remain separately labeled.
