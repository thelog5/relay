# Load test results

The resume numbers belong to the deployed instance (see the README). The runs
below are from a laptop and only show that the scripts work and roughly where
Relay sits.

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
