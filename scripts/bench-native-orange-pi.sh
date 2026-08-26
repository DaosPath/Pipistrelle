#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${ROOT}/target/release/pipistrelle-bench"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="${ROOT}/bench-results/native-${STAMP}"
FULL="${FULL:-0}"
mkdir -p "${OUT}"

cd "${ROOT}"
echo "Building native ARM64 benchmark..."
cargo build --release --bin pipistrelle-bench

curl -fsS http://127.0.0.1:9095/info > "${OUT}/broker-info.json"
uname -a > "${OUT}/system.txt"
lscpu >> "${OUT}/system.txt"
free -h >> "${OUT}/system.txt"

run_case() {
  local name="$1"; shift
  echo "== ${name} =="
  "${BIN}" "$@" --json-out "${OUT}/${name}.json"
}

# TCP QoS 0: end-to-end routing and raw ingest.
run_case tcp-loopback-c1-q0 \
  --mode loopback --clients 1 --messages 100000 --payload 128 --qos 0 --window 4096 --timeout 60
run_case tcp-loopback-c10-q0 \
  --mode loopback --clients 10 --messages 100000 --payload 128 --qos 0 --window 4096 --timeout 60
run_case tcp-loopback-c50-q0 \
  --mode loopback --clients 50 --messages 20000 --payload 128 --qos 0 --window 4096 --timeout 60
run_case tcp-ingest-c1-q0 \
  --mode ingest --clients 1 --messages 1000000 --payload 128 --qos 0 --window 16384 --timeout 60
run_case tcp-ingest-c10-q0 \
  --mode ingest --clients 10 --messages 100000 --payload 128 --qos 0 --window 8192 --timeout 60
run_case tcp-ingest-c50-q0 \
  --mode ingest --clients 50 --messages 20000 --payload 128 --qos 0 --window 8192 --timeout 60

# QoS 1 exposes ACK/persistence cost.
run_case tcp-loopback-c1-q1 \
  --mode loopback --clients 1 --messages 10000 --payload 128 --qos 1 --window 1024 --timeout 60
run_case tcp-loopback-c10-q1 \
  --mode loopback --clients 10 --messages 5000 --payload 128 --qos 1 --window 1024 --timeout 60
run_case tcp-ingest-c10-q1 \
  --mode ingest --clients 10 --messages 20000 --payload 128 --qos 1 --window 2048 --timeout 60

# TLS comparison. The negotiated group is included in each JSON result.
run_case tls-hybrid-loopback-c10-q0 \
  --tls --tls-profile hybrid --ca config/cert.pem \
  --mode loopback --clients 10 --messages 20000 --payload 128 --qos 0 --window 4096 --timeout 60
run_case tls-classical-loopback-c10-q0 \
  --tls --tls-profile classical --ca config/cert.pem \
  --mode loopback --clients 10 --messages 20000 --payload 128 --qos 0 --window 4096 --timeout 60

if [[ "${FULL}" == "1" ]]; then
  echo "== sustained 50M ingest =="
  run_case tcp-ingest-c50-q0-50m \
    --mode ingest --clients 50 --messages 1000000 --payload 128 --qos 0 --window 16384 --timeout 120

  echo "== sustained 10M loopback =="
  run_case tcp-loopback-c10-q0-10m \
    --mode loopback --clients 10 --messages 1000000 --payload 128 --qos 0 --window 16384 --timeout 120
fi

curl -fsS http://127.0.0.1:9095/metrics > "${OUT}/metrics-after.txt"
docker stats --no-stream pipistrelle_broker_test > "${OUT}/docker-stats-after.txt" 2>/dev/null || true

echo "Native benchmark results: ${OUT}"
