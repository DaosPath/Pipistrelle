# Security Policy

## Supported versions

Security fixes are targeted at the latest published V2 release and the active `v2` branch. Older tags are immutable historical releases and may not receive backports unless the issue is severe and a safe backport is practical.

## Reporting a vulnerability

Please **do not publish exploitable details in a public issue**. Use GitHub's private vulnerability reporting / Security Advisory flow for this repository when available (`Security` → `Report a vulnerability`). If the private reporting button is unavailable, open a public issue containing only a request for a private security contact channel and no sensitive reproduction details.

Useful reports include the affected version/commit, deployment mode, impact, minimal reproduction, relevant logs with secrets removed, and whether the issue can be triggered remotely or requires authenticated/local access.

## Security model

Pipistrelle is designed to fail closed for missing/invalid credentials, uses Argon2id password hashes, bounded queues/backpressure, TLS 1.3, MQTT protocol validation and persistent-session ownership binding. CI additionally runs RustSec dependency auditing and CodeQL analysis.

Pipistrelle does **not** currently claim complete MQTT v5 conformance or formal verification. See `docs/MQTT5_COMPLIANCE.md` for the explicit protocol support matrix and known remaining work.

## Secrets

Never commit production credentials, private keys, bridge passwords, `.env` files or production database snapshots. The repository intentionally versions examples only; local secrets remain ignored by Git.
