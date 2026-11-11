# Deploying the demo on Render

`render.yaml` at the repo root is a Render Blueprint that puts the whole demo
on Render for about $13 a month: no AWS account, no server to manage, and it
never sleeps.

## What it creates and costs

| Resource | Plan | Price | What it is |
|---|---|---|---|
| `relayqueue` | Starter web service (0.5 CPU, 512 MB) | $7/month | the api plus 3 supervised worker processes, built from the `Dockerfile` |
| `relay-db` | Postgres 16, Basic-256mb | $6/month | jobs and job events; no expiry, point-in-time recovery |
| `relay-kv` | Key Value (Valkey, Redis-compatible), Free | $0 | streams, retry set, pub/sub; 25 MB, in-memory |
| Workspace | Hobby | $0 | includes 2 custom domains and 5 GB of bandwidth a month |

Paid services are billed by the second, so deleting them stops the charge.

## Steps

1. Push this repo to GitHub. Render reads it through its GitHub app.
2. In the Render dashboard, add a payment method (**Workspace Settings >
   Billing**). The Blueprint creates paid services, so it cannot be applied
   without one.
3. **New > Blueprint**, connect GitHub, pick the repo. Render lists the three
   resources above. **Apply.** The first build takes a few minutes.
4. The dashboard is then at `https://relayqueue.onrender.com` (Render adds a
   short suffix if that name is taken).
5. Open it, click **Send 60 slow jobs first**, then **Kill a random worker**.

## Custom domain

The Hobby workspace includes two custom domains, and Render issues and renews
the HTTPS certificate itself.

1. Buy the domain at any registrar (Cloudflare Registrar and Porkbun sell at
   cost; a `.dev` is about $12 a year). `.dev` sites only load over HTTPS,
   which Render provides.
2. In Render: **relayqueue > Settings > Custom Domains > Add**, enter the
   domain (for example `relayqueue.dev`). Render also adds `www.` and
   redirects it to the bare domain.
3. At the registrar, add the DNS records Render shows. For a bare domain:
   - Cloudflare DNS: a `CNAME` for `@` to `relayqueue.onrender.com` (DNS only,
     grey cloud; Cloudflare flattens it), and a `CNAME` for `www` to the same.
   - Other DNS hosts: an `A` record for `@` to the IP Render shows, and a
     `CNAME` for `www` to `relayqueue.onrender.com`.
   - Remove any `AAAA` records for the domain; Render does not serve IPv6.
4. Click **Verify** in Render. The certificate is usually ready within a few
   minutes of DNS updating.

To keep the domain in the Blueprint as well, add it under the `relayqueue`
service in `render.yaml`:

```yaml
    domains:
      - relayqueue.dev
```

## How it differs from `docker compose up`

| | Compose (local, EC2) | Render |
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

## Behaviour to expect

- **Always on.** Paid instances do not sleep, so the first visitor gets the
  dashboard straight away.
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
- **Bandwidth.** The Hobby workspace includes 5 GB a month, then $0.15/GB.
  The dashboard is a few hundred KB per visit plus a small SSE stream, so a
  normal portfolio demo stays well inside it.

## Upgrades, if you want them

| Change | Extra cost | Why |
|---|---|---|
| `relay-kv` to `starter` (256 MB, disk-backed) | +$10/month | jobs survive a Key Value restart without the reconciler re-queuing them |
| `relay-db` to `basic-1gb` (0.5 CPU, 1 GB) | +$13/month | Postgres is the bottleneck under load (see the AWS results) |
| `relayqueue` to `standard` (1 CPU, 2 GB) | +$18/month | more headroom for the api and the three workers |
| Workers as their own `type: worker` service (`dockerCommand: worker`), several instances | $7 per instance | matches the compose layout; a killed worker then comes back when Render restarts its instance, which takes longer than the supervisor's one second |

Change the `plan:` in `render.yaml` and push; Render applies Blueprint
changes on sync.

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
