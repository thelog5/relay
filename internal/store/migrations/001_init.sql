-- Durable record of every job and every status change (R7).

CREATE TABLE IF NOT EXISTS jobs (
    id           uuid        PRIMARY KEY,
    type         text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status       text        NOT NULL,
    attempts     integer     NOT NULL DEFAULT 0,
    max_attempts integer     NOT NULL DEFAULT 5,
    last_error   text,
    worker_id    text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT jobs_status_check
        CHECK (status IN ('queued', 'running', 'retry_scheduled', 'succeeded', 'dead')),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts BETWEEN 1 AND 100)
);

-- GET /jobs?status=dead and the dashboard feed page by status, newest first.
CREATE INDEX IF NOT EXISTS jobs_status_updated_idx ON jobs (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS job_events (
    id          bigserial   PRIMARY KEY,
    job_id      uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    from_status text,
    to_status   text        NOT NULL,
    worker_id   text,
    error       text,
    at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events (job_id, id);
CREATE INDEX IF NOT EXISTS job_events_at_idx ON job_events (at DESC);
