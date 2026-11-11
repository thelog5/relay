# Load test results

The resume numbers belong to the deployed instance (see the README). The runs
below are from a laptop and only show that the scripts work and roughly where
Relay sits.

## AWS: t3.small in us-east-2, Nov 11, 2026

One t3.small (2 burstable vCPU, 2 GiB, Ubuntu) created by `infra/terraform`,
running the same `docker compose` stack: Postgres, Redis, api, 3 workers × 8
goroutines, Prometheus, Grafana. k6 ran on a laptop over the internet. Raw
output: [`results/t3.small-20261111-053133`](results/t3.small-20261111-053133)
(throughput, container CPU) and
[`results/t3.small-20261111-053718`](results/t3.small-20261111-053718)
(latency, crash recovery).

### Throughput: ramp toward 6,000 req/s

| Jobs sent and processed | Failed requests | Time to drain | Throughput |
|------------------------:|----------------:|--------------:|-----------:|
| 97,608 | 0 | 216 s | **452 jobs/s** end to end |

No request failed: past capacity, `POST /jobs` slows down (p99 4.8 s at the
top of the ramp) instead of erroring, and the backlog drains afterwards.

### Where the CPU went (sampled over AWS SSM during the ramp)

| Container | CPU (100% = one core) |
|-----------|----------------------:|
| postgres | 94–105% |
| api | 34–36% |
| redis | 23–28% |
| each worker | 11–18% |

Load average reached 32 on 2 vCPUs. Postgres is the limit: every job is four
writes (insert plus three status changes, each with an event row).

### Enqueue latency: steady 300 req/s for 60 s

| | p50 | p95 | p99 |
|---|---:|---:|---:|
| Server-side (the api's own histogram, via Prometheus) | 5.9 ms | 24.6 ms | **77.5 ms** |
| Client-side (k6 on the laptop, includes the internet) | 34.8 ms | 60.4 ms | 113.1 ms |

300 req/s is about two-thirds of measured capacity. The same test at 1,000
req/s (above capacity) measured queueing instead: p99 2.3 s, 0 errors.

### Crash recovery and loss

- `relayctl crash`: the killed worker held 8 jobs; the last finished on
  another worker **7.28 s** after the kill (limit 15 s).
- After all runs: 158,308 jobs succeeded, 0 dead, 0 unfinished.

## Local: MacBook Air, Docker in a 4-CPU / 6 GB Colima VM, Nov 7, 2026

Everything (Postgres, Redis, api, 3 workers × 8 goroutines, Prometheus,
Grafana) ran in one VM; k6 ran on the same laptop and reached the API through
Colima's port forwarding. Database and Redis emptied before the throughput run.

### p99 enqueue latency: `latency.js`, 1,000 req/s for 60s

| Requests | Failed | p50 | p95 | p99 | max |
|---------:|-------:|----:|----:|----:|----:|
| 60,001 | 0 | 1.34 ms | 4.39 ms | **39.85 ms** | 138.76 ms |

### Throughput: `throughput.js`, ramp to 4,000 req/s until errors

| Sent | Reached the API | Accepted (202) | Processed end to end | Time to drain | Throughput |
|-----:|----------------:|---------------:|---------------------:|--------------:|-----------:|
| 224,543 | 223,199 | 223,199 | 223,199 | 148.0 s | **≈1,508 jobs/s** |

- Every request that reached the API got a 202, and every accepted job
  succeeded: 0 stuck, 0 lost (`relay_http_requests_total{code="202"}` and the
  `succeeded` count both read 223,199).
- The 1,344 failed requests (1.09%, which ended the ramp) never reached the
  API: they died between k6 and the container, most likely in Colima's
  user-space port forwarder. The ceiling here is the laptop's networking as
  much as Relay.
- POST /jobs p99 over the whole ramp was 579 ms because the last stages
  queue up behind that bottleneck; compare the steady-rate run above.

## How to reproduce

```bash
docker compose exec -T postgres psql -U relay -d relay -c 'TRUNCATE jobs CASCADE'
docker compose exec -T redis redis-cli FLUSHDB && docker compose restart api worker
k6 run -e RELAY_URL=http://localhost:8080 -e LABEL=local -e MAX_RATE=4000 loadtest/throughput.js
k6 run -e RELAY_URL=http://localhost:8080 -e LABEL=local -e RATE=1000 loadtest/latency.js
```

Against the deployed instance, run from your laptop (not on the instance) the
command that `terraform output load_test` prints, adding `KILL_TOKEN=<token>`
if you set one. It runs the throughput ramp and the steady latency test, reads
the api's own latency from Prometheus (no internet round trip), samples each
container's CPU over AWS SSM during the ramp, and runs the crash-recovery
check. Everything lands in `loadtest/results/<label>-<timestamp>/` with a
`SUMMARY.md`; add a section above this one from it.
