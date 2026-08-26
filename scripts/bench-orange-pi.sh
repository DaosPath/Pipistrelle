#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${ROOT}/.venv/bin/python"
OUT_DIR="${ROOT}/bench-results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "${OUT_DIR}"

if [[ ! -x "${PYTHON}" ]]; then
  echo "Missing ${PYTHON}; create a venv and install paho-mqtt first." >&2
  exit 2
fi

VERSION="$(tr -d '\n' < "${ROOT}/VERSION")"
echo "Pipistrelle ${VERSION} Orange Pi benchmark -> ${OUT_DIR}"

for clients in 1 10 50; do
  for qos in 0 1; do
    name="tcp-c${clients}-q${qos}-p128"
    echo "== ${name} =="
    "${PYTHON}" "${ROOT}/tools/benchmark.py" \
      --clients "${clients}" --messages 1000 --payload 128 --qos "${qos}" \
      --json-out "${OUT_DIR}/${name}.json"
  done
done

docker stats --no-stream pipistrelle_broker_test > "${OUT_DIR}/docker-stats.txt" 2>/dev/null || true
curl -fsS http://127.0.0.1:9095/info > "${OUT_DIR}/broker-info.json" 2>/dev/null || true
uname -a > "${OUT_DIR}/system.txt"
printf 'cpu_count=%s\n' "$(nproc)" >> "${OUT_DIR}/system.txt"
free -h >> "${OUT_DIR}/system.txt"

echo "Benchmark complete: ${OUT_DIR}"
