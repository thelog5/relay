#!/usr/bin/env bash
# Measure a Relay deployment. Everything lands in one folder,
# loadtest/results/<label>-<timestamp>/, with a SUMMARY.md at the end:
#
#   1. throughput   k6 ramps POST /jobs until >1% fail; jobs/s processed end to end
#   2. latency      k6 at a steady RATE/s; p50/p95/p99 as the client sees them
#   3. server-side  the api's own POST /jobs p50/p95/p99 from Prometheus, without
#                   the internet round trip between your laptop and the server
#   4. containers   CPU and memory per container, sampled during the ramp over
#                   AWS SSM, to show which part runs out first (needs the aws CLI
#                   and INSTANCE_ID; skipped otherwise)
#   5. crash        relayctl kills a busy worker; seconds until its jobs finish
#                   elsewhere (needs KILL_TOKEN if the deployment sets one)
#
#   loadtest/run.sh http://<ip>:8080 t3.small
#   INSTANCE_ID=i-0abc123 KILL_TOKEN=secret loadtest/run.sh http://<ip>:8080 t3.small
#
# Run it from your laptop, not from the server under test, or k6 competes with
# Relay for the same CPU.
set -euo pipefail
cd "$(dirname "$0")/.."
URL=${1:-http://localhost:8080}
LABEL=${2:-local}
MAX_RATE=${MAX_RATE:-6000}
RATE=${RATE:-1000}
export DURATION=${DURATION:-60s} # steady latency run; also the Prometheus window
REGION=${AWS_REGION:-us-east-1}
HOST=$(echo "$URL" | sed -E 's#^[a-z]+://##; s#[:/].*$##')
PROM=${PROM_URL:-http://$HOST:9090}
OUT=loadtest/results/$LABEL-$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"
echo "results -> $OUT"

curl -sf "$URL/healthz" >/dev/null || { echo "no healthy Relay at $URL" >&2; exit 1; }

# ---- 4. container sampler, in the background during the ramp ---------------
SAMPLER=""
if [ -n "${INSTANCE_ID:-}" ] && command -v aws >/dev/null; then
  cat > "$OUT/.ssm-params.json" <<'JSON'
{"commands": ["uptime", "docker stats --no-stream --format '{{.Name}}  cpu {{.CPUPerc}}  mem {{.MemUsage}}'"]}
JSON
  (
    for _ in $(seq 1 10); do
      cid=$(aws ssm send-command --region "$REGION" --instance-ids "$INSTANCE_ID" \
        --document-name AWS-RunShellScript --parameters "file://$OUT/.ssm-params.json" \
        --query Command.CommandId --output text 2>/dev/null) || break
      sleep 8
      { echo "== $(date +%T)"
        aws ssm get-command-invocation --region "$REGION" --command-id "$cid" --instance-id "$INSTANCE_ID" \
          --query StandardOutputContent --output text 2>/dev/null; } >> "$OUT/containers.txt" || true
      sleep 7
    done
  ) &
  SAMPLER=$!
  echo "sampling container CPU on $INSTANCE_ID every ~15s"
else
  echo "container sampling skipped (set INSTANCE_ID and install the aws CLI to enable)"
fi

# ---- 1. throughput ----------------------------------------------------------
echo "== 1/5 throughput ramp up to ${MAX_RATE}/s"
k6 run -q -e RELAY_URL="$URL" -e LABEL="$LABEL" -e MAX_RATE="$MAX_RATE" -e OUT_DIR="$OUT" \
  loadtest/throughput.js 2>&1 | tee "$OUT/throughput.txt" || true
[ -n "$SAMPLER" ] && { kill "$SAMPLER" 2>/dev/null || true; wait "$SAMPLER" 2>/dev/null || true; }

# ---- 2. latency -------------------------------------------------------------
echo "== 2/5 enqueue latency at ${RATE}/s"
k6 run -q -e RELAY_URL="$URL" -e LABEL="$LABEL" -e RATE="$RATE" -e OUT_DIR="$OUT" \
  loadtest/latency.js 2>&1 | tee "$OUT/latency.txt" || true

# ---- 3. server-side latency over exactly the steady run --------------------
echo "== 3/5 server-side latency from $PROM"
prom_ms() {
  curl -sf -G "$PROM/api/v1/query" --data-urlencode \
    "query=histogram_quantile($1, sum by (le) (rate(relay_http_request_duration_seconds_bucket{route=\"POST /jobs\"}[$DURATION])))" |
    python3 -c 'import sys, json; r = json.load(sys.stdin)["data"]["result"]; print("%.2f" % (float(r[0]["value"][1]) * 1000) if r else "n/a")' 2>/dev/null || echo "n/a"
}
{ echo "p50_ms $(prom_ms 0.5)"; echo "p95_ms $(prom_ms 0.95)"; echo "p99_ms $(prom_ms 0.99)"; } | tee "$OUT/server-latency.txt"

# ---- 5. crash recovery ------------------------------------------------------
echo "== 5/5 crash recovery"
go build -o bin/relayctl ./cmd/relayctl
./bin/relayctl crash -url "$URL" 2>&1 | tee "$OUT/crash.txt" || true

# ---- summary ----------------------------------------------------------------
python3 - "$OUT" "$URL" "$LABEL" "$RATE" <<'PY'
import glob, json, os, re, sys
out, url, label, rate = sys.argv[1:5]
def load(name):
    files = sorted(glob.glob(os.path.join(out, f"{name}-*.json")))
    return json.load(open(files[-1])) if files else {}
def text(name):
    p = os.path.join(out, name)
    return open(p).read().strip() if os.path.exists(p) else ""
t, l = load("throughput"), load("latency")
server = dict(line.split() for line in text("server-latency.txt").splitlines() if len(line.split()) == 2)
crash = re.search(r"crash result:.*", text("crash.txt"))
lines = [f"# Relay load test: {label}", "", f"Target: {url}", "",
    "| Measure | Result |", "|---|---|",
    f"| Jobs processed end to end | {t.get('jobs processed', 'n/a')} |",
    f"| Throughput (processed/s, end to end) | {t.get('processed/s (end to end)', 'n/a')} |",
    f"| Drained after (s) | {t.get('drained after (s)', 'n/a')} |",
    f"| Ramp failure rate | {t.get('failure rate', 'n/a')} |",
    f"| Enqueue latency at {rate}/s, client p50 / p95 / p99 (ms) | {l.get('p50 (ms)', 'n/a')} / {l.get('p95 (ms)', 'n/a')} / {l.get('p99 (ms)', 'n/a')} |",
    f"| Enqueue latency, server-side p50 / p95 / p99 (ms, from histogram buckets) | {server.get('p50_ms', 'n/a')} / {server.get('p95_ms', 'n/a')} / {server.get('p99_ms', 'n/a')} |",
    f"| Crash recovery | {crash.group(0) if crash else 'n/a'} |", ""]
cont = text("containers.txt")
if cont:
    lines += ["## Containers during the ramp", "", "```", cont, "```", ""]
open(os.path.join(out, "SUMMARY.md"), "w").write("\n".join(lines))
print("\n".join(lines))
PY
echo "saved $OUT/SUMMARY.md"
