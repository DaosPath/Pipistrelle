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
