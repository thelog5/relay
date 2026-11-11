package jobs

import (
	"encoding/json"
	"time"
)

// DefaultMaxAttempts is used when a job is enqueued without max_attempts (R1).
const DefaultMaxAttempts = 5

// Job is the durable record of one unit of work. It mirrors the jobs table.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      Status          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	LastError   string          `json:"last_error,omitempty"`
	WorkerID    string          `json:"worker_id,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	// PrevWorkerID is set by store.Transition: the worker that held the job
	// before that transition. A reclaim names the crashed worker with it.
	PrevWorkerID string `json:"-"`
}

// Event is one row of the job_events audit log (R7).
type Event struct {
	ID         int64     `json:"id"`
	JobID      string    `json:"job_id"`
	FromStatus Status    `json:"from_status"`
	ToStatus   Status    `json:"to_status"`
	WorkerID   string    `json:"worker_id,omitempty"`
	Error      string    `json:"error,omitempty"`
	At         time.Time `json:"at"`
}
