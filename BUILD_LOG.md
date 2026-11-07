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
