# Relay load test: t3.small

Target: http://77.113.160.17:8080

| Measure | Result |
|---|---|
| Jobs processed end to end | n/a |
| Throughput (processed/s, end to end) | n/a |
| Drained after (s) | n/a |
| Ramp failure rate | n/a |
| Enqueue latency at 300/s, client p50 / p95 / p99 (ms) | 34.84 / 60.42 / 113.05 |
| Enqueue latency, server-side p50 / p95 / p99 (ms, from histogram buckets) | 5.86 / 24.56 / 77.53 |
| Crash recovery | crash result: 8 orphaned jobs, last finished 7.28s after the kill (limit 15s) |
