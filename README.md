# Relay

A fault-tolerant job queue in Go that never loses a job when a worker crashes,
with a live dashboard that proves it.

Submit work over HTTP; run it across worker processes; retry failures with
exponential backoff; dead-letter what keeps failing. Kill a worker in the
middle of a job from the dashboard and watch a live worker pick up its jobs a
few seconds later.

<!-- TODO(logan): record docs/demo.gif — jobs flowing, worker killed, jobs recovered -->
![Relay dashboard: a worker is killed mid-job and its jobs are rescued](docs/demo.gif)

```
docker compose up --build
open http://localhost:8080      # dashboard
```

## Delivery guarantee: at-least-once

**A job is never lost, but a crash can make it run twice.** Job handlers must
be idempotent.

A job runs twice when a worker dies after doing the work but before recording
that it finished, or when it stalls long enough for its lease to expire and
another worker takes the job over. Relay limits the damage but cannot remove it:

- Each attempt is recorded in Postgres as `running` with a `worker_id`. A
  result is only accepted from the worker that currently owns the job
  (fencing), so a slow worker whose job was taken over cannot also mark it
  finished.
- A finished job that is delivered again (crash between the Postgres write
  and the Redis ack) is recognised and acknowledged without running.
- The `webhook` job sends an `Idempotency-Key` header equal to the job ID on
  every attempt, so the receiver can drop duplicates.

Exactly-once would need the handler's side effect and Relay's bookkeeping in
one transaction, which an external webhook can never offer.

## How it works

```mermaid
flowchart LR
  client([client]) -- "POST /jobs" --> api
  subgraph api [api process]
    http[HTTP + SSE] --- sched[retry scheduler<br/>every 500ms]
    http --- recon[reconciler<br/>every 30s]
  end
  api -- "1. INSERT job (queued)" --> pg[(Postgres<br/>jobs, job_events)]
  api -- "2. XADD" --> stream[[relay:jobs<br/>stream + group]]
  stream -- XREADGROUP --> w1[worker 1]
  stream -- XREADGROUP --> w2[worker 2]
  stream -- XREADGROUP --> w3[worker 3]
  w1 & w2 & w3 -- "status + event" --> pg
  w1 & w2 & w3 -- "failed: ZADD" --> retry[(relay:retry<br/>zset by run-at)]
  w1 & w2 & w3 -- "exhausted: XADD" --> dead[[relay:dead]]
  sched -- "due: ZREM + XADD" --> stream
  w1 & w2 & w3 -. "PUBLISH" .-> events((relay:events)) -. SSE .-> dash[dashboard]
```

Redis decides **what still needs to run**; Postgres records **what happened**.
Every status change writes the `jobs` row and a `job_events` row (timestamp
and worker ID) in a single statement.

### Job lifecycle

```mermaid
stateDiagram-v2
  [*] --> queued: POST /jobs
  queued --> running: worker starts attempt
  running --> succeeded
  running --> retry_scheduled: failed, attempts left
  retry_scheduled --> queued: backoff elapsed (scheduler)
  running --> dead: attempts exhausted / permanent error
  running --> running: reclaimed from a crashed worker
  dead --> queued: retry from dashboard
```

Retries back off 1s, 2s, 4s, … capped at 60s, plus up to 20% jitter. Jobs
default to 5 attempts. Errors a retry cannot fix (bad payload, webhook 4xx)
skip straight to `dead`, and the last error is kept on the job and on the
`relay:dead` stream.

### Crash recovery

A worker that crashes leaves its jobs in its Redis pending list,
unacknowledged. They are recovered in two ways:

| Path | Trigger | Typical time after the crash |
|------|---------|------------------------------|
| Fast path | Survivors check the worker registry every 1s. A worker whose heartbeat is 6s stale is dead; its pending jobs are `XCLAIM`ed once idle for two lease periods. | ~6–8s |
| Safety net (PRD design) | Every 5s each worker runs `XAUTOCLAIM` with a 10s idle threshold, catching anything, including consumers that never registered. | ≤15s |

Running jobs **renew their lease** every 3s (an atomic Lua check-and-`XCLAIM`
by the owner), so a healthy job that runs longer than the 10s threshold is not
stolen. That was the PRD's main risk; the README was meant to "mention
heartbeats as the production fix", and they turned out cheap enough to build.

Orphaned jobs get capacity before new work, so recovery is not stuck behind a
backlog. If a worker dies during a job's **final** attempt, the job goes to
`dead` rather than running again; a job that crashes its worker every time
cannot take the fleet down forever.

### Write order: what if a process dies right here?

| Step | Dies right after | Outcome |
|------|------------------|---------|
| api: INSERT job | before XADD | Reconciler finds it `queued` and missing from Redis after 30s and enqueues it |
| worker: mark `running` | before running the handler | Message is still pending; reclaimed and run (attempt counted) |
| worker: run handler | mid-job (`os.Exit(1)`) | Reclaimed and run again: **the at-least-once duplicate** |
| worker: mark `succeeded` | before XACK | Redelivered, seen as finished, acked without running |
| worker: mark `retry_scheduled` | before ZADD+XACK | Reclaimed and run again (Postgres accepts `retry_scheduled → running`) |
| worker: ZADD + XACK + XDEL | n/a, one atomic Lua script | — |
| scheduler: mark `queued` | before ZREM+XADD | Still in the retry set; next tick moves it |

## Numbers

| Metric | How measured | Result |
|--------|--------------|--------|
| Throughput (jobs/sec) | k6, `sleep` jobs at 0 ms, ramp until errors (`loadtest/throughput.js`) | **TODO** on the deployed instance · laptop: ≈1,508 jobs/s end to end |
| p99 enqueue latency (ms) | k6 on `POST /jobs` (`loadtest/latency.js`) | **TODO** on the deployed instance · laptop: 39.9 ms at 1,000 req/s |
| Time to recover a crashed job (s) | Crash test timestamps in `job_events` | **7.5s** kill → orphans finished (e2e test, PRD timers); ~6–7s reclaim gap in compose |
| Jobs lost in crash tests | Count in `jobs` vs jobs sent | **0** of 2,000 (kill all 3 workers) and 0 of 300 (SIGKILL all, restart) |

Instance type: **TODO** (fill in after `terraform apply`; numbers depend on hardware).

Laptop runs (MacBook Air, Docker in a 4-CPU Colima VM, k6 on the same
laptop) are in [`loadtest/RESULTS.md`](loadtest/RESULTS.md): 223,199 jobs
accepted and processed with 0 lost; the ramp stopped on connection errors
between k6 and the VM, not on errors from Relay.

Acceptance runs against `docker compose`:

- **10,000 `flaky` jobs at 30% failure:** 9,972 succeeded, 28 dead (≈24
  expected after 5 attempts), **0 stuck**, settled in 22s.
- **Kill a worker mid-job:** all 8 jobs it held finished on another worker
  8.3s after the kill (`relayctl crash`); the automated e2e test sees 7.5s.
- **Kill all workers:** 2,000 of 2,000 jobs succeeded after Docker restarted
  every worker; 0 lost.

## API

| Method and path | Purpose |
|-----------------|---------|
| `POST /jobs` | Enqueue `{"type", "payload", "max_attempts"?}`; returns `202 {"id"}` |
| `GET /jobs/:id` | Job status plus its full event history |
| `GET /jobs?status=dead&limit=50` | List jobs by status, newest first |
| `POST /jobs/:id/retry` | Re-queue a dead job with a fresh attempt budget |
| `DELETE /jobs/:id` | Delete a dead or succeeded job |
| `GET /stats` | Counts per status and per worker, queue depth, recovery timings |
| `GET /events` | Server-sent events for the dashboard (`stats` every 1s, `jobs` batched every 200ms) |
| `POST /workers/:id/kill` | Demo only: that worker calls `os.Exit(1)` mid-job. Needs `?token=` when `KILL_TOKEN` is set |
| `GET /metrics` | Prometheus metrics (each worker also serves its own on `:9091`) |
| `GET /healthz` | Postgres and Redis reachability |

Job types:

| Type | Payload | Behaviour |
|------|---------|-----------|
| `sleep` | `{"ms": 250}` | Waits, then succeeds |
| `flaky` | `{"fail_rate": 0.3, "ms": 50}` | Fails each attempt with the given probability |
| `webhook` | `{"url": "https://…", "body": {…}, "timeout_ms": 5000}` | POSTs `body`; 2xx succeeds, 5xx/408/429/network errors retry, other 4xx go dead |

```bash
curl -s localhost:8080/jobs -d '{"type":"flaky","payload":{"fail_rate":0.5},"max_attempts":3}'
curl -s localhost:8080/jobs/<id> | jq
```

## Running it

| Command | What it does |
|---------|--------------|
| `make up` | `docker compose up` the stack: Postgres 16, Redis 7, api, 3 workers, Prometheus, Grafana |
| `make soak` / `make crash` / `make chaos` | Acceptance checks against the running stack via `relayctl` |
| `make load URL=http://host:8080 LABEL=t3.small` | k6 throughput and latency runs |
| `make test-deps && make test` | Unit, integration and e2e crash tests against throwaway containers |
| `make lint` | golangci-lint |

Dashboard <http://localhost:8080> · Grafana <http://localhost:3000> ·
Prometheus <http://localhost:9090>.

### Configuration

| Variable | Default | Used by |
|----------|---------|---------|
| `DATABASE_URL` | `postgres://relay:relay@localhost:5432/relay?sslmode=disable` | api, worker |
| `REDIS_URL` | `redis://localhost:6379/0` | api, worker |
| `HTTP_ADDR` | `:8080` | api |
| `KILL_TOKEN` | empty (kill endpoint open) | api |
| `SCHEDULER_INTERVAL` | `500ms` | api |
| `RECONCILE_INTERVAL` / `RECONCILE_GRACE` | `30s` / `30s` | api |
| `WORKER_CONCURRENCY` | `8` goroutines per process | worker |
| `RECLAIM_INTERVAL` / `RECLAIM_IDLE` | `5s` / `10s` (XAUTOCLAIM) | worker |
| `LEASE_INTERVAL` | `3s` (`0` disables leases and the fast path) | worker |
| `HEARTBEAT_INTERVAL` / `DEAD_AFTER` | `2s` / `6s` | worker |
| `JOB_TIMEOUT` | `60s` | worker |
| `WORKER_ID` | hostname + random suffix | worker |
| `RELAY_KEY_PREFIX` | `relay` | api, worker |

## Tests

- **Unit:** lifecycle, backoff bounds and jitter, handler behaviour, webhook status mapping.
- **Integration** (real Postgres and Redis, a throwaway database and key prefix per test): atomic
  transitions and fencing, Lua moves, autoclaim, leases, retries settling with no job stuck, dead-letter,
  reclaim from a crashed consumer, orphans jumping the backlog, the API end to end including SSE.
- **E2E crash tests** (`internal/e2e`): build the real worker binary, run three
  processes with the PRD timers, kill one through `POST /workers/:id/kill`
  and assert its jobs finish elsewhere within 15s; SIGKILL every worker,
  start new ones, assert zero loss.
- **CI** (`.github/workflows/ci.yml`): race-enabled tests against Postgres and
  Redis service containers, golangci-lint, and the Docker build on every push.

## Deploy

Terraform puts the whole compose stack on one EC2 instance (`infra/terraform`):

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # set admin_cidr and kill_token
terraform init && terraform apply
terraform output load_test                     # the k6 command for this instance
```

`t3.small` costs about $15 a month; `terraform destroy` once the GIF and the
numbers are recorded. A Fly.io fallback lives in `infra/fly`.

## Trade-offs and what production would add

- **Redis is a single point of failure.** AOF with `fsync everysec` bounds
  loss on a Redis crash to about a second of queue operations; production
  would run Redis with replicas and Sentinel, or a managed service.
- **Postgres is written on every transition** (~4 writes per job), which
  caps throughput but gives a full audit trail. Batching event inserts is
  the next lever.
- **The reconciler scans the stream** when it finds stale `queued` jobs. A
  transactional outbox would close the insert/XADD gap without scanning.
- Not built, per the PRD: priorities, scheduled or cron jobs, authentication,
  multi-region, Kubernetes, gRPC.

## Repo layout

```
cmd/api           HTTP server, retry scheduler, reconciler
cmd/worker        worker process
cmd/relayctl      acceptance checks: soak, crash, chaos
internal/jobs     job model, lifecycle, backoff, handlers
internal/queue    Redis streams, retry set, dead letters, pub/sub, worker registry
internal/store    Postgres store and migrations
internal/worker   worker runtime: fetch/reclaim loop, leases, fencing
internal/api      HTTP handlers, SSE hub, stats
internal/scheduler retry promotion and reconciliation
internal/e2e      crash tests against real worker processes
web/dashboard.html live dashboard (embedded into the api binary)
loadtest/         k6 scripts
infra/            Terraform, Fly.io, Prometheus, Grafana
```
