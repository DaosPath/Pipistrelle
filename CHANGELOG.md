# Changelog

Pipistrelle uses a public four-component release version (`MAJOR.MINOR.PATCH.BUILD`) stored in `VERSION`. Cargo keeps standard three-component SemVer internally.

## [2.1.2.3] - 2026-08-28

### Performance

- Added a Linux/AArch64 SVE gather fast path for the 9-byte QoS0 Topic Alias structural scanner, with runtime SVE detection and exact scalar fallback on mismatches.
- Removed an unnecessary Tokio writer mutex from the QoS0 native benchmark path while preserving full receive-side validation.
- Improved the Topic Alias end-to-end ceiling from the `2.1.2.2` ~35.2M msg/s release median into the ~38–39M msg/s tuning range on the Orange Pi. The best validated short run observed during release tuning was 38.981M msg/s; benchmark categories remain explicitly separated from ingest/sendfile ceilings.

### Safety and correctness

- Reworked fast alias expansion so `Vec<u8>` length is set only after all output bytes are initialized.
- Scoped retained-state restoration so a synchronous lock is never held across asynchronous database deletion awaits.
- Retained all MQTT Topic Alias mapping/remap/reset semantics, bounded backpressure and malformed-packet fallback behavior.

### Engineering

- Added GitHub Actions CI for formatting, Clippy, release Rust tests, Docker integration, raw MQTT v5 protocol tests and destructive restart/persistence tests.
- Added RustSec and CodeQL security workflows.
- Added Dependabot coverage for Cargo, GitHub Actions and Docker dependencies.
- Added `SECURITY.md`, a pinned Rust 1.98.0 toolchain, release/tag version consistency checks and expanded project/release documentation.
- Removed the unmaintained direct `rustls-pemfile` dependency (RUSTSEC-2025-0134) and migrated certificate/key loading to the maintained `rustls-pki-types::pem::PemObject` APIs.

### Release gates

- **52/52** Rust tests.
- Clippy correctness/suspicious/`await_holding_lock` gate clean.
- **RustSec audit clean** across 159 locked dependencies.
- **6/6** transport/auth/ACL/TLS/WebSocket/Prometheus integration checks.
- **25/25** raw MQTT v5 protocol scenarios.
- **10/10** destructive SIGKILL/restart persistence scenarios.
- Exact Docker image reports `2.1.2.3` from `/health`.

## [2.1.2.2] - 2026-08-27

- Added bidirectional MQTT v5 Topic Alias routing, outbound mapping establishment/reuse and alias epoch cache invalidation.
- Sustained ~35.184M msg/s median Topic Alias end-to-end across three ~2B-message runs with exact Prometheus accounting.
- Release gates: 51/51 Rust, 25/25 raw MQTT v5, 10/10 SIGKILL/restart and 6/6 integration.
