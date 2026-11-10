// Package store persists jobs and their status history in Postgres (R7).
//
// Postgres is the system of record for what happened to a job. Redis (see
// internal/queue) is the source of truth for what still needs to run. Every
// status change writes the jobs row and a job_events row in one statement.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thelog5/relay/internal/jobs"
)

// ErrNotFound means no job has the given ID.
var ErrNotFound = errors.New("job not found")

// ConflictError means the job was not in a status the caller expected, so
// the transition was not applied. Current holds the status it actually has.
type ConflictError struct {
	ID      string
	Current jobs.Status
	Want    []jobs.Status
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("job %s is %s, expected one of %v", e.ID, e.Current, e.Want)
}

// Store wraps a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to Postgres and verifies the connection.
func Open(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s := &Store{pool: pool}
	if err := s.waitReady(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// waitReady retries Ping for a while so services can start before Postgres
// finishes booting under docker compose.
func (s *Store) waitReady(ctx context.Context) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := s.pool.Ping(ctx)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("postgres not ready: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Close releases all connections.
func (s *Store) Close() { s.pool.Close() }

// Ping checks connectivity, for health checks.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const jobColumns = `id::text, type, payload, status, attempts, max_attempts,
	COALESCE(last_error, ''), COALESCE(worker_id, ''), created_at, updated_at`

func scanJob(row pgx.Row) (*jobs.Job, error) {
	var j jobs.Job
	var status string
	var payload []byte
	if err := row.Scan(&j.ID, &j.Type, &payload, &status, &j.Attempts, &j.MaxAttempts,
		&j.LastError, &j.WorkerID, &j.CreatedAt, &j.UpdatedAt); err != nil {
		return nil, err
	}
	j.Status = jobs.Status(status)
	j.Payload = json.RawMessage(payload)
	return &j, nil
}

// CreateJob inserts a job in status queued, plus its first event.
func (s *Store) CreateJob(ctx context.Context, id, typ string, payload json.RawMessage, maxAttempts int) (*jobs.Job, error) {
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	row := s.pool.QueryRow(ctx, `
		WITH ins AS (
			INSERT INTO jobs (id, type, payload, status, max_attempts)
			VALUES ($1::uuid, $2, $3::jsonb, 'queued', $4)
			RETURNING *
		), ev AS (
			INSERT INTO job_events (job_id, from_status, to_status, at)
			SELECT id, NULL, 'queued', created_at FROM ins
		)
		SELECT `+jobColumns+` FROM ins`,
		id, typ, string(payload), maxAttempts)
	return scanJob(row)
}

// GetJob loads one job.
func (s *Store) GetJob(ctx context.Context, id string) (*jobs.Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1::uuid`, id)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

// JobEvents returns a job's status history, oldest first.
func (s *Store) JobEvents(ctx context.Context, id string) ([]jobs.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, job_id::text, COALESCE(from_status, ''), to_status,
		       COALESCE(worker_id, ''), COALESCE(error, ''), at
		FROM job_events WHERE job_id = $1::uuid ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanEvent)
}

func scanEvent(row pgx.CollectableRow) (jobs.Event, error) {
	var e jobs.Event
	var from, to string
	err := row.Scan(&e.ID, &e.JobID, &from, &to, &e.WorkerID, &e.Error, &e.At)
	e.FromStatus, e.ToStatus = jobs.Status(from), jobs.Status(to)
	return e, err
}

// ListFilter selects jobs for GET /jobs.
type ListFilter struct {
	Status jobs.Status // empty = any
	Limit  int
}

// ListJobs returns jobs newest-updated first.
func (s *Store) ListJobs(ctx context.Context, f ListFilter) ([]jobs.Job, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 50
	}
	var rows pgx.Rows
	var err error
	if f.Status != "" {
		rows, err = s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE status = $1
			ORDER BY updated_at DESC LIMIT $2`, string(f.Status), f.Limit)
	} else {
		rows, err = s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs
			ORDER BY updated_at DESC LIMIT $1`, f.Limit)
	}
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (jobs.Job, error) {
		j, err := scanJob(r)
		if err != nil {
			return jobs.Job{}, err
		}
		return *j, nil
	})
}

// CountByStatus returns how many jobs are in each status. Every status is
// present in the map, zero-filled.
func (s *Store) CountByStatus(ctx context.Context) (map[jobs.Status]int64, error) {
	out := make(map[jobs.Status]int64, len(jobs.AllStatuses))
	for _, st := range jobs.AllStatuses {
		out[st] = 0
	}
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[jobs.Status(st)] = n
	}
	return out, rows.Err()
}

// TransitionOpts describes one status change.
type TransitionOpts struct {
	// From lists the statuses the job may currently be in. If it is in any
	// other status, nothing changes and a *ConflictError is returned.
	From []jobs.Status
	To   jobs.Status
	// WorkerID is recorded on the event and, if non-empty, on the job.
	WorkerID string
	// Error is recorded on the event and, if non-empty, as last_error.
	Error string
	// IncAttempts adds one to attempts (used when a worker starts a run).
	IncAttempts bool
	// ResetAttempts sets attempts back to zero (manual retry of a dead job).
	ResetAttempts bool
	// OwnedBy, if set, additionally requires the job's current worker_id to
	// match. Workers use it as a fence: once another worker reclaims a job,
	// the original (slow, not dead) worker can no longer finish it.
	OwnedBy string
}

// Transition atomically moves a job between statuses and appends the
// matching job_events row, in a single round trip.
func (s *Store) Transition(ctx context.Context, id string, o TransitionOpts) (*jobs.Job, jobs.Status, error) {
	from := make([]string, len(o.From))
	for i, st := range o.From {
		from[i] = string(st)
	}
	inc := 0
	if o.IncAttempts {
		inc = 1
	}
	reset := o.ResetAttempts
	var prev string
	row := s.pool.QueryRow(ctx, `
		WITH prev AS (
			SELECT id, status, worker_id FROM jobs WHERE id = $1::uuid FOR UPDATE
		), upd AS (
			UPDATE jobs j SET
				status     = $2,
				attempts   = CASE WHEN $7 THEN 0 ELSE j.attempts + $3 END,
				worker_id  = COALESCE(NULLIF($4, ''), j.worker_id),
				last_error = COALESCE(NULLIF($5, ''), j.last_error),
				updated_at = clock_timestamp()
			FROM prev
			WHERE j.id = prev.id
			  AND prev.status = ANY($6::text[])
			  AND ($8 = '' OR prev.worker_id = $8)
			RETURNING j.*, prev.status AS prev_status, prev.worker_id AS prev_worker
		), ev AS (
			-- running -> running is a reclaim: note who the job was taken from.
			INSERT INTO job_events (job_id, from_status, to_status, worker_id, error, at)
			SELECT id, prev_status, status, NULLIF($4, ''),
			       CASE WHEN prev_status = 'running' AND status = 'running'
			            THEN 'reclaimed from ' || COALESCE(prev_worker, 'unknown worker')
			            ELSE NULLIF($5, '') END,
			       updated_at
			FROM upd
		)
		SELECT `+jobColumns+`, prev_status FROM upd`,
		id, string(o.To), inc, o.WorkerID, o.Error, from, reset, o.OwnedBy)

	var j jobs.Job
	var status string
	var payload []byte
	err := row.Scan(&j.ID, &j.Type, &payload, &status, &j.Attempts, &j.MaxAttempts,
		&j.LastError, &j.WorkerID, &j.CreatedAt, &j.UpdatedAt, &prev)
	if errors.Is(err, pgx.ErrNoRows) {
		cur, gerr := s.GetJob(ctx, id)
		if gerr != nil {
			return nil, "", gerr
		}
		return cur, cur.Status, &ConflictError{ID: id, Current: cur.Status, Want: o.From}
	}
	if err != nil {
		return nil, "", err
	}
	j.Status = jobs.Status(status)
	j.Payload = json.RawMessage(payload)
	return &j, jobs.Status(prev), nil
}

// RequeueDue moves jobs from retry_scheduled to queued, for the scheduler.
// IDs that are no longer retry_scheduled are skipped. Returns the IDs moved.
func (s *Store) RequeueDue(ctx context.Context, ids []string, actor string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		WITH upd AS (
			UPDATE jobs SET status = 'queued', updated_at = clock_timestamp()
			WHERE id = ANY($1::uuid[]) AND status = 'retry_scheduled'
			RETURNING id, updated_at
		), ev AS (
			INSERT INTO job_events (job_id, from_status, to_status, worker_id, at)
			SELECT id, 'retry_scheduled', 'queued', $2, updated_at FROM upd
		)
		SELECT id::text FROM upd`, ids, actor)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// StaleUnfinished returns IDs of jobs that are queued, running or waiting to
// retry and whose last status change is older than cutoff. The reconciler
// checks these against Redis; any that Redis has no trace of get re-enqueued.
func (s *Store) StaleUnfinished(ctx context.Context, cutoff time.Time, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id::text FROM jobs
		WHERE status IN ('queued', 'running', 'retry_scheduled') AND updated_at < $1
		ORDER BY updated_at LIMIT $2`, cutoff, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// DeleteJob removes a job and its events (R12). Only terminal jobs can be
// deleted so a running job never disappears from under a worker.
func (s *Store) DeleteJob(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jobs WHERE id = $1::uuid AND status IN ('dead', 'succeeded')`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		cur, err := s.GetJob(ctx, id)
		if err != nil {
			return err
		}
		return &ConflictError{ID: id, Current: cur.Status, Want: []jobs.Status{jobs.StatusDead, jobs.StatusSucceeded}}
	}
	return nil
}

// FeedEvent is a job_events row joined with the job type, for the dashboard.
type FeedEvent struct {
	jobs.Event
	Type string `json:"type"`
}

// RecentEvents returns the newest status changes across all jobs.
func (s *Store) RecentEvents(ctx context.Context, limit int) ([]FeedEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.id, e.job_id::text, COALESCE(e.from_status, ''), e.to_status,
		       COALESCE(e.worker_id, ''), COALESCE(e.error, ''), e.at, j.type
		FROM job_events e JOIN jobs j ON j.id = e.job_id
		ORDER BY e.id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (FeedEvent, error) {
		var fe FeedEvent
		var from, to string
		err := r.Scan(&fe.ID, &fe.JobID, &from, &to, &fe.WorkerID, &fe.Error, &fe.At, &fe.Type)
		fe.FromStatus, fe.ToStatus = jobs.Status(from), jobs.Status(to)
		return fe, err
	})
}

// RecoveryStats measures crash recovery from job_events: for every job that
// was reclaimed (running -> running), the time from the reclaim back to the
// previous running event, and from the reclaim to success.
type RecoveryStats struct {
	Reclaimed      int64   `json:"reclaimed"`
	AvgReclaimSecs float64 `json:"avg_reclaim_secs"`
	MaxReclaimSecs float64 `json:"max_reclaim_secs"`
}

// Recovery computes RecoveryStats over all reclaim events.
func (s *Store) Recovery(ctx context.Context) (RecoveryStats, error) {
	var rs RecoveryStats
	err := s.pool.QueryRow(ctx, `
		WITH r AS (
			SELECT e.at - (
				SELECT max(p.at) FROM job_events p
				WHERE p.job_id = e.job_id AND p.id < e.id AND p.to_status = 'running'
			) AS gap
			FROM job_events e
			WHERE e.from_status = 'running' AND e.to_status = 'running'
		)
		SELECT count(*),
		       COALESCE(EXTRACT(EPOCH FROM avg(gap)), 0),
		       COALESCE(EXTRACT(EPOCH FROM max(gap)), 0)
		FROM r`).Scan(&rs.Reclaimed, &rs.AvgReclaimSecs, &rs.MaxReclaimSecs)
	return rs, err
}
