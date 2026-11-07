#!/usr/bin/env bash
# Run both load tests against a Relay instance and save JSON summaries in
# loadtest/results/.
#
#   loadtest/run.sh http://<host>:8080 t3.small
#
# Run k6 from a separate machine (or at least not the box under test) when you
# record numbers for the README; otherwise k6 competes with Relay for CPU.
set -euo pipefail
cd "$(dirname "$0")/.."
URL=${1:-http://localhost:8080}
LABEL=${2:-local}
MAX_RATE=${MAX_RATE:-6000}
RATE=${RATE:-500}

echo "== throughput ramp against $URL ($LABEL), up to ${MAX_RATE}/s"
k6 run -q -e RELAY_URL="$URL" -e LABEL="$LABEL" -e MAX_RATE="$MAX_RATE" loadtest/throughput.js || true

echo "== p99 enqueue latency at ${RATE}/s"
k6 run -q -e RELAY_URL="$URL" -e LABEL="$LABEL" -e RATE="$RATE" loadtest/latency.js || true
