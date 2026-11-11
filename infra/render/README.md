# Deploying the demo on Render

`render.yaml` at the repo root is a Render Blueprint for the public demo:
the app on Render, Postgres on Neon's free plan. It costs about $7 a month,
needs no server to manage, and never sleeps.

## What it uses and costs

| Resource | Plan | Price | What it is |
|---|---|---|---|
| `relayqueue` (Render) | Starter web service (0.5 CPU, 512 MB) | $7/month | the api plus 3 supervised worker processes, built from the `Dockerfile` |
| `relay-kv` (Render) | Key Value (Valkey, Redis-compatible), Free | $0 | streams, retry set, pub/sub; 25 MB, in-memory |
| Postgres (Neon) | Free | $0 | jobs and job events; 100 compute-hours a month, scales to zero after 5 idle minutes |
| Render workspace | Hobby | $0 | 5 GB of bandwidth a month included |

Render bills paid services by the second, so deleting one stops the charge.

## 1. Create the Neon database

1. Sign up at [neon.com](https://neon.com) and create a project:
   - **Postgres version:** 16 or newer.
   - **Cloud and region:** AWS, **US West 2 (Oregon)**, the same region as
     the Render services. Each job makes four Postgres writes, so the round
     trip matters.
2. On the project dashboard click **Connect**, turn **Connection pooling
   off**, and copy the connection string. It looks like
   `postgresql://user:password@ep-something.us-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require`.

   Use the direct string (no `-pooler` in the host). Relay's migrations hold
   a Postgres advisory lock for the length of a session, which Neon's pooler
   (PgBouncer in transaction mode) does not keep. The demo opens at most 32
   connections, well under Neon's limit.

## 2. Deploy on Render

1. Add a payment method in **Workspace Settings > Billing**. The web service
   is on a paid plan, so the Blueprint cannot be applied without one.
2. **New > Blueprint**, connect GitHub, pick this repo. Render lists
   `relayqueue` and `relay-kv` and asks for `DATABASE_URL`: paste the Neon
   string. It is stored in Render, never in the repo.
3. **Apply.** The first build takes a few minutes. The api creates the
   tables on startup.
4. Open `https://relayqueue.onrender.com`, click **Send 60 slow jobs first**,
   then **Kill a random worker**.

**About the `.onrender.com` address:** Render sets the subdomain from the
service name when the service is created, and it cannot be changed later.
If another account holds `relayqueue`, Render adds a random suffix instead.
If that happens, delete the service and apply again with a different `name:`
in `render.yaml` before you share the link.

## How Neon stays inside the free plan

Neon only charges compute while queries run, and suspends the database after
5 minutes without any. Left alone, the api would query Postgres every second
for dashboard stats, and Render's health check pings it too, so Neon would
never sleep and the free 100 compute-hours would run out around the middle
of each month.

`IDLE_AFTER=2m` fixes that. Once there has been no open dashboard, no new or
retried job and no unfinished job for 2 minutes, the api stops its stats
queries, the reconciler and purger skip their passes, and `/healthz` checks
only Redis (it answers `"postgres": "idle"`). Neon then suspends 5 minutes
later. The next visitor or `POST /jobs` wakes it; the first query takes a few
hundred milliseconds longer. A typical month of demo traffic uses a few
compute-hours.

Idle mode only starts when Postgres holds no unfinished jobs, so a job is
never left waiting on a skipped reconciler pass.

Optional: in Neon's compute settings, cap autoscaling at 0.25 or 0.5 CU so a
burst of jobs cannot spend the month's hours quickly.

## How it differs from `docker compose up`

| | Compose (local, EC2) | Render + Neon |
|---|---|---|
| Processes | api container + 3 worker containers | one container: `allinone` runs the api and 3 worker processes |
| Killed worker comes back via | Docker's `restart: unless-stopped` | the supervisor in `cmd/allinone`, about a second later |
| Worker goroutines | 8 each | 4 each |
| Redis persistence | AOF, fsync every second | none (free Key Value is in-memory) |
| Postgres | always-on container | Neon, sleeps when the demo is quiet |
| Prometheus + Grafana | included | not deployed; `/metrics` is still served |
| Public-demo limits | off | on (`DEMO_MODE=true`) |

Each worker is still a separate OS process with its own Redis consumer, so
a kill from the dashboard really does orphan that worker's jobs and the
others really do reclaim them.

## Behaviour to expect

- **Always on.** The Render service does not sleep. After a quiet spell only
  the database does, and it wakes in under a second.
- **Each push to `main` redeploys.** Render builds the new image and switches
  over once `/healthz` passes. The old workers get SIGTERM and up to 20
  seconds to finish their jobs; anything still running after that is
  reclaimed by the new workers.
- **Key Value keeps no data across restarts.** If it restarts, workers
  recreate the consumer group and the reconciler puts every job that
  Postgres still has as unfinished back on the queue within about 30 seconds.
  Nothing is lost; some jobs may run twice, which at-least-once delivery
  allows. Finished jobs are deleted from the stream, so 25 MB is plenty for
  the demo's 20,000-job cap.
- **Storage.** Neon's free plan has a storage cap per project; finished jobs
  are purged after 6 hours, so the demo stays far below it.
- **Bandwidth.** The Hobby workspace includes 5 GB a month, then $0.15/GB.
  A dashboard visit is a few hundred KB plus a small SSE stream.

## Upgrades, if you want them

| Change | Extra cost | Why |
|---|---|---|
| `relay-kv` to `starter` (256 MB, disk-backed) | +$10/month | jobs survive a Key Value restart without the reconciler re-queuing them |
| Render Postgres instead of Neon (`basic-256mb`) | +$6/month | always on, same network as the app; drop `IDLE_AFTER` |
| `relayqueue` to `standard` (1 CPU, 2 GB) | +$18/month | more headroom for the api and the three workers |
| Workers as their own `type: worker` service (`dockerCommand: worker`), several instances | $7 per instance | matches the compose layout; a killed worker then comes back when Render restarts its instance, which takes longer than the supervisor's one second |

Change the plan in `render.yaml` and push; Render applies Blueprint changes
on sync. A custom domain can be added later under **relayqueue > Settings >
Custom Domains**; the Hobby workspace includes two.

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
set `KILL_TOKEN` on the `relayqueue` service and the dashboard will ask for
it.
