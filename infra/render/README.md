# Deploying the demo on Render

`render.yaml` at the repo root is a Render Blueprint that puts the whole demo
on Render's free plan: no AWS account, no server to manage.

## Steps

1. Push this repo to GitHub. It can stay private; Render reads it through
   its GitHub app.
2. In the Render dashboard: **New > Blueprint**, connect GitHub, pick the
   repo. Render shows the three resources from `render.yaml`:
   - `relay`: the web service, built from the `Dockerfile`
   - `relay-kv`: Key Value (Valkey, Redis-compatible)
   - `relay-db`: Postgres 16
3. **Apply.** The first build takes a few minutes. The dashboard is then at
   `https://relay-<something>.onrender.com`.
4. Open it, click **Send 60 slow jobs first**, then **Kill a random worker**.

## How it differs from `docker compose up`

| | Compose (local, EC2) | Render free |
|---|---|---|
| Processes | api container + 3 worker containers | one container: `allinone` runs the api and 3 worker processes |
| Killed worker comes back via | Docker's `restart: unless-stopped` | the supervisor in `cmd/allinone`, about a second later |
| Worker goroutines | 8 each | 4 each |
| Redis persistence | AOF, fsync every second | none (free Key Value is in-memory) |
| Prometheus + Grafana | included | not deployed; `/metrics` is still served |
| Public-demo limits | off | on (`DEMO_MODE=true`) |

Each worker is still a separate OS process with its own Redis consumer, so
a kill from the dashboard really does orphan that worker's jobs and the
others really do reclaim them.

## Free-tier behaviour to expect

- **Sleeps after 15 minutes without traffic.** The next visitor waits about
  a minute while it starts again. Open the link yourself shortly before you
  show it to someone.
- **Free Postgres expires 30 days after it is created.** Render emails you
  first. Upgrade it to a paid plan, or delete `relay-db` and apply the
  Blueprint again for a fresh one.
- **Key Value keeps no data across restarts.** If it restarts, workers
  recreate the consumer group and the reconciler puts every job that
  Postgres still has as unfinished back on the queue within about 30 seconds.
  Nothing is lost; some jobs may run twice, which at-least-once delivery
  allows.
- **750 free instance hours a month** per workspace. One always-awake
  service uses about 744.

## Public-demo protections (`DEMO_MODE=true`)

| Limit | Value | Override with |
|---|---|---|
| New jobs per visitor address | 1,200 a minute, bursts of 600 | `RATE_LIMIT_JOBS_PER_MIN`, `RATE_LIMIT_JOBS_BURST` |
| Worker kills per visitor address | 12 a minute | `RATE_LIMIT_KILLS_PER_MIN` |
| Unfinished jobs before new ones are refused | 20,000 | `MAX_UNFINISHED_JOBS` |
| Webhook jobs | off, so visitors cannot make the server call URLs | `DISABLED_JOB_TYPES` |
| Longest sleep job | 10 seconds | `MAX_SLEEP_MS` |
| Finished jobs kept | 6 hours | `RETENTION` |

The kill button is open to visitors on purpose; that is the demo. To lock it,
set `KILL_TOKEN` on the `relay` service and the dashboard will ask for it.

## Paid alternative

Background workers are not free on Render, but on a paid plan you can run
the api as a web service and workers as a separate `type: worker` service
(`dockerCommand: worker`) with several instances, which matches the compose
layout. The free Blueprint above is the cheapest way to get a live link.
