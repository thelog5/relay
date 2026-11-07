# Fly.io fallback

Untested: written as the PRD's fallback in case the EC2 path stalls. Steps:

1. `fly postgres create --name relay-pg --region iad` then
   `fly postgres attach relay-pg --app relay-demo` (sets `DATABASE_URL`).
2. Redis 7 with Streams and Lua: run a tiny app from `redis:7-alpine` with a
   volume (`fly launch --image redis:7-alpine --name relay-redis`), then
   `fly secrets set REDIS_URL=redis://relay-redis.internal:6379/0 --app relay-demo`.
   Upstash Redis may work too, but check it supports `XAUTOCLAIM` and `EVALSHA`.
3. `fly secrets set KILL_TOKEN=<secret> --app relay-demo`
4. `fly deploy --config infra/fly/fly.toml` and `fly scale count api=1 worker=3`.

The dashboard's worker kill button works the same way: the worker exits with
status 1 and Fly restarts the machine.
