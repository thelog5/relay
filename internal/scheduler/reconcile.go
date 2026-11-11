package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/thelog5/relay/internal/metrics"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/store"
)

// Reconciler repairs the gaps between Postgres and Redis that the write
// order cannot close by itself:
//
//   - a job row committed as queued whose XADD never happened (the api
//     crashed between the two writes);
//   - unfinished jobs whose Redis entries vanished because Redis restarted
//     without persistence (free hosted instances do this).
//
// Every Interval it looks at queued, running and retrying jobs that have not
// changed for longer than Grace and puts any that Redis has no trace of (not
// in the stream, not in the retry set) back on the stream. A healthy running
// job is always still in the stream's pending list, so it is left alone; a
// job that finishes between the two checks is acked as a duplicate by the
// worker's status fencing.
type Reconciler struct {
	Store    *store.Store
	Queue    *queue.Queue
	Interval time.Duration // default 30s
	Grace    time.Duration // default 30s; must exceed the enqueue round trip
	Batch    int           // default 1000
	// Idle, if set, skips passes while it returns true. The api passes its
	// own Idle, which is only true while Postgres holds no unfinished jobs,
	// so a skipped pass would have found nothing.
	Idle   func() bool
	Logger *slog.Logger
}

// Run reconciles until ctx is cancelled, starting immediately so an api that
// restarts after a crash repairs its own half-finished enqueues.
func (r *Reconciler) Run(ctx context.Context) {
	if r.Interval <= 0 {
		r.Interval = 30 * time.Second
	}
	if r.Logger == nil {
		r.Logger = slog.Default()
	}
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		if r.Idle == nil || !r.Idle() {
			if n, err := r.Once(ctx, time.Now()); err != nil && ctx.Err() == nil {
				r.Logger.Warn("reconcile failed", "err", err)
			} else if n > 0 {
				r.Logger.Warn("re-enqueued jobs that were missing from redis", "count", n)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Once runs a single pass and returns how many jobs it re-enqueued.
func (r *Reconciler) Once(ctx context.Context, now time.Time) (int, error) {
	grace, batch := r.Grace, r.Batch
	if grace <= 0 {
		grace = 30 * time.Second
	}
	if batch <= 0 {
		batch = 1000
	}
	ids, err := r.Store.StaleUnfinished(ctx, now.Add(-grace), batch)
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	present, err := r.Queue.Outstanding(ctx, ids)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		if present[id] {
			continue
		}
		if _, err := r.Queue.Enqueue(ctx, id); err != nil {
			return n, err
		}
		n++
	}
	metrics.JobsReconciled.Add(float64(n))
	return n, nil
}
