-- Speeds up crash-recovery stats (running -> running events are reclaims).
CREATE INDEX IF NOT EXISTS job_events_reclaim_idx
    ON job_events (job_id, id)
    WHERE from_status = 'running' AND to_status = 'running';
