# Build log

One row per block of the PRD, appended the moment that block was finished.
Times are local (America/New_York). Every block also has its own commit(s), so
`git log --reverse --format='%h %ad %s' --date=format:'%Y-%m-%d %H:%M'` lines up with this table.

Open questions and blockers live in the "Relay Build — Open Questions & Blockers" doc.

| # | Finished | Block | PRD refs | Notes |
|---|----------|-------|----------|-------|
| 1 | 2026-11-07 04:15 | Repo bootstrap: README, .gitignore, build log, Go module | - | Installed Go 1.27, Redis, k6, golangci-lint via Homebrew (no Docker on this machine) |
| 2 | 2026-11-07 04:15 | Job model + lifecycle state machine | Job lifecycle | queued/running/retry_scheduled/succeeded/dead; running->running = reclaim |
| 3 | 2026-11-07 04:15 | Retry backoff with jitter | R4 | 1s,2s,4s... cap 60s, +0-20% jitter |
| 4 | 2026-11-07 04:17 | Job handlers: sleep, flaky, webhook | R2 | Webhook sends Idempotency-Key=job ID; 4xx (except 408/429) = permanent -> dead; blocks 169.254.x |
| 5 | 2026-11-07 04:20 | Postgres store: schema, migrator, guarded transitions, event log | R7 | Single-statement transition+event; tests use a throwaway DB per test |
| 6 | 2026-11-07 04:24 | Redis queue: stream+group, retry zset, dead stream, pub/sub, worker registry | R4 R5 R6 R8 | Atomic Lua moves; lease-extend heartbeat script; XAUTOCLAIM reclaim |
| 7 | 2026-11-07 04:29 | Worker runtime + retry scheduler | R3 R4 R5 R6 | In-process tests: 300 flaky jobs settle, crash reclaim <3s with fast timers, lease extension, poison-pill guard |
| 8 | 2026-11-07 04:36 | HTTP API + api/worker binaries + live kill smoke test | R1 R8 R9 R12 | Live test found reclaim starvation -> rewrote worker loop; added heartbeat-based fast rescue (reclaim ~6s after kill) |
| 9 | 2026-11-07 04:41 | Live dashboard page (SSE) | R8 R12 | Counts, workers + kill buttons, recovery gap, throughput sparkline, feed, dead-letter retry/delete, burst buttons; verified via headless Chrome screenshot |
| 10 | 2026-11-07 04:43 | Docker image + docker compose stack (api, 3 workers, pg, redis, prometheus, grafana) | R10 R13 | Verified: compose up healthy; killed worker auto-restarted; 8 orphaned jobs rescued in 6.7s; Prometheus scrapes api + 3 workers; Grafana dashboard provisioned |
| 11 | 2026-11-07 04:48 | relayctl acceptance runs against compose: soak, crash, chaos | Acceptance 1-3 | 10k flaky@30%: 9,972 ok / 28 dead / 0 stuck in 22s. Crash: 8 orphans done 8.3s after kill. Chaos (kill all 3): 2,000/2,000, 0 lost |
| 12 | 2026-11-07 04:48 | Fix: orphaned jobs jump the backlog | R6 | Chaos run exposed 17s rescue under backlog; now ~7s; regression test fails on old loop |
| 13 | 2026-11-07 04:49 | Automated e2e crash tests with real processes | Acceptance 2-3 | Kill via API mid-job: orphans done 7.5s after kill (<15s). SIGKILL all + restart: 300/300, 0 lost |
| 14 | 2026-11-07 04:57 | CI workflow, lint config, Terraform (EC2), Fly fallback, Makefile | R11 Acceptance 5 | Terraform validated with OpenTofu; NOT applied (needs AWS creds). CI not yet run on GitHub (not pushed) |
| 15 | 2026-11-07 05:00 | Fix: enqueue stuck on client disconnect + reconciler | R1 R6 | Found by k6: 24 jobs stuck queued after request timeouts. Enqueue now detached from client ctx; reconciler re-enqueues queued-but-missing jobs (repaired the 24 on restart) |
| 16 | 2026-11-07 05:02 | README: guarantee, architecture, recovery design, API, config, numbers | PRD README asks | Deployed-instance numbers + GIF left as TODO for Logan |
| 17 | 2026-11-07 06:45 | Clean full test run + laptop load test results | Numbers table | All 9 packages pass with -race. Laptop: 1,508 jobs/s end to end (223,199 jobs, 0 lost), p99 39.9ms @1k req/s. Deployed numbers still TODO |
| 18 | 2026-11-07 06:48 | Demo GIF recorded (scripted) + KILLED state on dashboard | Acceptance 6 | docs/demo.gif, 2.8 MB, 19s: kill via dashboard button, 8 jobs rescued avg 6.5s. Re-record anytime: node scripts/record-demo.mjs |
| 19 | 2026-11-10 18:27 | Dashboard redesign (black, professional) + GIF re-recorded | R8 | Sans-serif ops layout, worker job-slot view, axis chart with hover, real tables, token dialog; GIF 2.8MB -> 256KB |
| 20 | 2026-11-10 23:52 | Render deploy: allinone supervisor, demo mode, Redis-loss recovery, restyled UI | R8, R10, R11 (alt host) | render.yaml free Blueprint; NOGROUP recovery + reconciler for running/retrying jobs; rate limits, queue cap, webhook off, retention; Satoshi/lavender dashboard |
