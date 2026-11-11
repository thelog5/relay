# Relay

**A job queue in Go that never loses a job when a worker crashes, with a live dashboard where you kill workers yourself and watch their jobs move.**

![Relay dashboard: a worker is killed mid-job and the others pick up its jobs](docs/demo.gif)

**Run it with `docker compose up`** · Built by [Logan Li](https://www.linkedin.com/in/logan-li-9089b7216/)

## What it is

Apps hand slow work to a job queue so users don't wait for it: sending an
email, calling another service, resizing an image. The hard part is what
happens when something breaks. Relay takes jobs over HTTP, saves each one in
Postgres, queues it in Redis Streams, and runs it on separate worker
processes.

- **Retries:** a failed job waits 1s, 2s, 4s and so on (up to 60s) and tries again.
- **Dead letters:** a job that keeps failing is set aside, to retry or delete from the dashboard.
- **Crash recovery:** if a worker dies in the middle of a job, the other workers take its jobs within seconds. Nothing is lost.
- **Live dashboard:** every job and worker updating in real time, with a kill button on each worker so you can test the recovery yourself.

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

1. **Enqueue.** `POST /jobs` writes the job to Postgres as `queued`, then adds
   its ID to a Redis stream, and answers `202` right away.
2. **Run.** Three worker processes, each running 8 jobs at a time, read the
   stream through one consumer group, so each job goes to one worker.
3. **Record.** Every status change updates the job row and appends an event
   row (time and worker) in a single SQL statement, so the history is
   complete.
4. **Retry or give up.** A failed job goes into a Redis sorted set keyed by
   when it may run again; the scheduler moves due jobs back onto the stream.
   A job out of attempts goes to the dead-letter stream.
5. **Recover.** Workers send heartbeats. When one goes quiet, the others
   claim the jobs it was holding.
6. **Show it.** Status changes are published over Redis and streamed to the
   dashboard with server-sent events.

Redis decides **what still needs to run**; Postgres records **what happened**.
The [design notes](#design-notes) below cover delivery guarantees, the job
lifecycle, crash recovery timing and what happens if a process dies between
any two writes.

## Results

Measured on AWS with the Terraform in [`infra/terraform`](infra/terraform):
one **t3.small** (2 burstable vCPU, 2 GiB) in us-east-2 running the same
`docker compose` stack, with k6 on a laptop over the internet (`make load`).

| Metric | How measured | t3.small on AWS | Laptop (4-CPU VM) |
|--------|--------------|-----------------|-------------------|
| Throughput | k6 ramps `POST /jobs` (`sleep` 0 ms) until errors; jobs finished end to end | **452 jobs/s** (97,608 jobs, 0 failed requests) | ≈1,508 jobs/s |
| p99 enqueue latency | `POST /jobs` at a steady rate below capacity; the api's own histogram, so no internet round trip | **77.5 ms** at 300 req/s (113 ms seen from the laptop) | 39.9 ms at 1,000 req/s |
| Crash recovery | `relayctl crash`: kill a worker holding 8 jobs, time until the last one finishes elsewhere | **7.3 s** (limit 15 s) | 8.3 s; e2e test 7.5 s |
| Jobs lost | Unfinished or missing jobs after every run | **0** of 158,308 | 0 of 223,199; 0 of 2,000 with all workers killed |

**The bottleneck is Postgres.** During the ramp it held a full core (94–105%
CPU) while the api used about 35%, each worker 12–18% and Redis about 25%.
Every job costs four Postgres writes (the insert and three status changes,
each with an event row), so batching the event inserts is the next lever,
then a connection pooler and a bigger instance. Above about 450 jobs/s new
jobs queue up rather than fail: a steady 1,000 req/s on this instance gave a
p99 of 2.3 s with zero errors.

Acceptance runs against `docker compose`:

- **10,000 `flaky` jobs at 30% failure:** 9,972 succeeded, 28 dead (about 24
  expected after 5 attempts), **0 stuck**, settled in 22s.
- **Kill a worker mid-job:** all 8 jobs it held finished on another worker
  8.3s after the kill; the automated e2e test sees 7.5s.
- **Kill all workers:** 2,000 of 2,000 jobs succeeded after Docker restarted
  every worker; 0 lost.

Raw output and the laptop runs: [loadtest/RESULTS.md](loadtest/RESULTS.md) and
[loadtest/results/](loadtest/results/).

## Engineering highlights

- **Proven by killing real processes.** The end-to-end tests build the real
  worker binary, run three of them, kill one mid-job with `os.Exit(1)` and
  assert its jobs finish elsewhere within 15 seconds; another test SIGKILLs
  every worker and asserts zero loss. On AWS, 0 of 158,308 jobs were lost.
- **Recovery in about 7 seconds, not 15.** With the spec's timers alone
  (claim jobs idle for 10s, checked every 5s) recovery took about 14s.
  Heartbeats let the survivors spot a dead worker after 6s and claim its jobs
  straight away, and orphaned jobs now jump ahead of new work: a chaos test
  had them waiting 17.4s behind a backlog before that fix.
- **No double finishes.** A worker can only record a result while Postgres
  still lists it as the job's owner, so a slow worker whose job was taken
  over cannot also mark it done.
- **Long jobs are not stolen.** A running job renews its lease every 3s with
  an atomic Lua check-and-claim, so a healthy 60-second job keeps its worker.
- **No gap between the database and the queue.** A load test caught 24 jobs
  stuck in `queued`: client timeouts cancelled the request between the
  Postgres insert and the Redis write. The enqueue now finishes even if the
  client hangs up, and a reconciler re-queues anything Postgres has that
  Redis does not, which also recovers every unfinished job if Redis restarts
  with no data.
- **A job that kills its worker cannot loop forever.** If a worker dies
  during a job's last attempt, the job goes to dead letters instead of
  crashing the next worker too.
- **Load-tested with the bottleneck found.** `make load` records throughput,
  client and server-side latency, per-container CPU over AWS SSM, and crash
  recovery into one report: Postgres saturates first at 452 jobs/s.
- **Ready for the public internet.** Demo mode adds per-visitor rate limits,
  a queue cap, a sleep cap and retention, and turns off webhook jobs so
  visitors cannot make the server call arbitrary URLs.
- **70 tests** across unit, integration (real Postgres and Redis, a fresh
  database per test) and end-to-end crash tests, all run with the race
  detector.

## Built with

Go · Redis Streams · PostgreSQL · Lua · Server-sent events · Prometheus ·
Grafana · Docker Compose · Terraform (AWS EC2, SSM) · k6 · GitHub Actions ·
Render

## Status

Complete and load-tested on AWS; the EC2 instance was deleted after the run
and `terraform apply` brings it back in a few minutes. The public demo runs
on Render from [`render.yaml`](render.yaml) for about $13 a month
([guide](infra/render/README.md)).

## Run it locally

Requires Docker.

```bash
docker compose up --build
open http://localhost:8080      # dashboard
```

Click **60 slow jobs**, then **Kill a random worker**, and watch the
handover. Grafana is at <http://localhost:3000> and Prometheus at
<http://localhost:9090>.

| Command | What it does |
|---------|--------------|
| `make up` | Start Postgres 16, Redis 7, the api, 3 workers, Prometheus and Grafana |
| `make soak` / `make crash` / `make chaos` | Acceptance checks against the running stack (`relayctl`) |
| `make load URL=http://host:8080 LABEL=t3.small` | Throughput, latency, container CPU and crash recovery into one report |
| `make test-deps && make test` | Unit, integration and e2e crash tests against throwaway containers |
| `make lint` | golangci-lint |

Enqueue a job yourself:

```bash
curl -s localhost:8080/jobs -d '{"type":"flaky","payload":{"fail_rate":0.5},"max_attempts":3}'
curl -s localhost:8080/jobs/<id> | jq
```

## Design notes

### Delivery guarantee: at-least-once

**A job is never lost, but a crash can make it run twice.** Job handlers must
be idempotent.

A job runs twice when a worker dies after doing the work but before recording
that it finished, or when it stalls long enough for its lease to expire and
another worker takes the job over. Relay limits the damage but cannot remove
it:

- Each attempt is recorded in Postgres as `running` with a `worker_id`. A
  result is only accepted from the worker that currently owns the job
  (fencing), so a slow worker whose job was taken over cannot also mark it
  finished.
- A finished job that is delivered again (a crash between the Postgres write
  and the Redis ack) is recognised and acknowledged without running.
- The `webhook` job sends an `Idempotency-Key` header equal to the job ID on
  every attempt, so the receiver can drop duplicates.

Exactly-once would need the handler's side effect and Relay's bookkeeping in
one transaction, which an external webhook can never offer.

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
| Safety net | Every 5s each worker runs `XAUTOCLAIM` with a 10s idle threshold, catching anything, including consumers that never registered. | ≤15s |

Running jobs renew their lease every 3s (an atomic Lua check-and-`XCLAIM` by
the owner), so a healthy job that runs longer than the 10s threshold is not
stolen. Orphaned jobs get capacity before new work, so recovery is not stuck
behind a backlog. If a worker dies during a job's final attempt, the job goes
to `dead` rather than running again.

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
| Redis restarts with no data | any time | Workers recreate the consumer group; the reconciler re-queues every job Postgres still has as unfinished |

### API

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

| Job type | Payload | Behaviour |
|----------|---------|-----------|
| `sleep` | `{"ms": 250}` | Waits, then succeeds |
| `flaky` | `{"fail_rate": 0.3, "ms": 50}` | Fails each attempt with the given probability |
| `webhook` | `{"url": "https://…", "body": {…}, "timeout_ms": 5000}` | POSTs `body`; 2xx succeeds, 5xx/408/429/network errors retry, other 4xx go dead |

### Configuration

| Variable | Default | Used by |
|----------|---------|---------|
| `DATABASE_URL` | `postgres://relay:relay@localhost:5432/relay?sslmode=disable` | api, worker |
| `REDIS_URL` | `redis://localhost:6379/0` | api, worker |
| `HTTP_ADDR` | `:$PORT` if `PORT` is set, else `:8080` | api |
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
| `DEMO_MODE` | off; turns on the public-demo limits and 6h retention | api |
| `RATE_LIMIT_JOBS_PER_MIN` / `RATE_LIMIT_JOBS_BURST` / `RATE_LIMIT_KILLS_PER_MIN` | off (demo: 1,200 / 600 / 12 per address) | api |
| `MAX_UNFINISHED_JOBS` / `MAX_SLEEP_MS` / `DISABLED_JOB_TYPES` | off (demo: 20,000 / 10,000 / `webhook`) | api |
| `TRUST_PROXY_HOPS` | `0`; `1` behind a platform proxy such as Render's | api |
| `RETENTION` | off (demo: `6h`); purges finished jobs older than this | api |
| `WORKERS` | `3` worker processes | allinone |
| `WORKER_REDIS_POOL_SIZE` | concurrency × 2 + 10 | worker |

### Tests

- **Unit:** lifecycle, backoff bounds and jitter, handler behaviour, webhook
  status mapping, rate limiting, the process supervisor.
- **Integration** (real Postgres and Redis, a throwaway database and key
  prefix per test): atomic transitions and fencing, Lua moves, autoclaim,
  leases, retries settling with no job stuck, dead letters, reclaim from a
  crashed consumer, orphans jumping the backlog, recovery after Redis loses
  its data, retention, and the API end to end including SSE.
- **End-to-end crash tests** (`internal/e2e`): build the real worker binary,
  run three processes, kill one through `POST /workers/:id/kill` and assert
  its jobs finish elsewhere within 15s; SIGKILL every worker, start new ones,
  assert zero loss.
- **CI** ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)):
  race-enabled tests against Postgres and Redis service containers,
  golangci-lint, and the Docker build.

### Deploy

**Public demo on Render.** [`render.yaml`](render.yaml) is a Render
Blueprint: New > Blueprint, pick this repo, apply. It runs `allinone` (the
api plus three supervised worker processes in one container) on a Starter
web service, with Basic-256mb Postgres, free Key Value and the public-demo
limits on. Steps, costs and the custom domain setup are in
[infra/render/README.md](infra/render/README.md).

**AWS EC2 for load tests.** Terraform puts the whole compose stack on one EC2
instance, reachable only from your IP:

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # set admin_cidr and kill_token
terraform init && terraform apply
terraform output load_test                     # the measurement command for this instance
terraform destroy                              # when the numbers are recorded
```

A `t3.small` costs about 2 cents an hour. A Fly.io fallback lives in
[`infra/fly`](infra/fly).

### Trade-offs and what production would add

- **Redis is a single point of failure.** AOF with `fsync everysec` bounds
  loss on a Redis crash to about a second of queue operations, and the
  reconciler rebuilds the queue from Postgres if Redis loses everything;
  production would run Redis with replicas, or a managed service.
- **Postgres is written on every transition** (about 4 writes per job),
  which caps throughput but gives a full audit trail. Batching event inserts
  is the next lever.
- **The reconciler scans the stream** when it finds stale unfinished jobs.
  A transactional outbox would close the insert/XADD gap without scanning.
- **The public demo is protected, not hardened.** A real launch would also
  need authentication and per-tenant authorization, HTTPS everywhere (Render
  terminates it; the EC2 setup does not), webhook egress through an
  allow-listed proxy, managed Postgres and Redis with backups, alerts on
  queue depth, dead letters and worker count, and more than one host.
- Not built: priorities, scheduled or cron jobs, multi-region, Kubernetes,
  gRPC.

## Repository layout

| Path | Contents |
| --- | --- |
| [`cmd/api`](cmd/api) | HTTP server, retry scheduler, reconciler, retention purge |
| [`cmd/worker`](cmd/worker) | Worker process |
| [`cmd/allinone`](cmd/allinone) | The api plus supervised workers in one container, for Render |
| [`cmd/relayctl`](cmd/relayctl) | Acceptance checks: soak, crash, chaos |
| [`internal/`](internal/) | Job model and handlers, Redis queue, Postgres store, worker runtime, API, scheduler, supervisor, e2e tests |
| [`web/`](web/) | The dashboard (one HTML file, embedded in the api binary) and its logo |
| [`loadtest/`](loadtest/) | k6 scripts, the measurement runner and [results](loadtest/RESULTS.md) |
| [`infra/`](infra/) | Terraform for AWS, the Render guide, Fly.io, Prometheus and Grafana config |
| [`scripts/`](scripts/) | Build log marker, demo GIF recorder, logo asset builder |
| [`BUILD_LOG.md`](BUILD_LOG.md) | When each block of the project was built |

## License

The code is under the [MIT License](LICENSE).
