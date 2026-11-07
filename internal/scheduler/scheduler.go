// Package scheduler moves due retries from the relay:retry sorted set back
// onto the job stream. It runs as one goroutine inside the api process,
// ticking every 500ms.
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/thelog5/relay/internal/metrics"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/store"
)

// ActorID is recorded as the worker_id on retry_scheduled -> queued events.
const ActorID = "scheduler"

// Scheduler promotes due retries.
type Scheduler struct {
	Store    *store.Store
	Queue    *queue.Queue
	Interval time.Duration // default 500ms
	Batch    int64         // max retries promoted per tick, default 1000
	Logger   *slog.Logger
}

// Run ticks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	if s.Interval <= 0 {
		s.Interval = 500 * time.Millisecond
	}
	if s.Batch <= 0 {
		s.Batch = 1000
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Drain everything due, a batch at a time, so a burst of retries
			// does not trickle out at Batch per 500ms.
			for {
				n, err := s.Tick(ctx, time.Now())
				if err != nil {
					if ctx.Err() == nil {
						s.Logger.Warn("scheduler tick failed", "err", err)
					}
					break
				}
				if n < int(s.Batch) {
					break
				}
			}
		}
	}
}

// Tick promotes retries due at now and returns how many were due.
//
// Order matters: Postgres flips retry_scheduled -> queued first, then Redis
// moves the IDs. If the process dies in between, the IDs are still in the
// retry set and the next tick finishes the move (the Postgres step is then a
// no-op). Redis's ZREM-guarded XADD means two schedulers never double-enqueue.
func (s *Scheduler) Tick(ctx context.Context, now time.Time) (int, error) {
	ids, err := s.Queue.DueRetries(ctx, now, s.Batch)
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	requeued, err := s.Store.RequeueDue(ctx, ids, ActorID)
	if err != nil {
		return 0, err
	}
	moved, err := s.Queue.PromoteRetries(ctx, ids)
	if err != nil {
		return 0, err
	}
	metrics.RetriesPromoted.Add(float64(moved))
	at := time.Now()
	for _, id := range requeued {
		_ = s.Queue.PublishEvent(ctx, queue.StatusEvent{
			JobID: id, From: "retry_scheduled", To: "queued", WorkerID: ActorID, At: at,
		})
	}
	return len(ids), nil
}
