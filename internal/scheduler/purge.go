package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/store"
)

// Purger deletes finished jobs (succeeded or dead) once they are older than
// Keep, along with their events and dead-letter stream entries, so a demo on
// a small hosted database (such as a free Neon project) never fills it.
// Unfinished jobs are never purged.
type Purger struct {
	Store    *store.Store
	Queue    *queue.Queue
	Keep     time.Duration
	Interval time.Duration // default 10m
	Batch    int           // default 5000 rows per delete
	// Idle, if set, holds purges back while it returns true, so a
	// serverless database can sleep; a held-back purge runs at the first
	// minute that is not idle.
	Idle   func() bool
	Logger *slog.Logger
}

// Run purges every Interval until ctx is cancelled.
func (p *Purger) Run(ctx context.Context) {
	if p.Interval <= 0 {
		p.Interval = 10 * time.Minute
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	// With Idle set, look every minute so a short visit still gets a purge
	// that came due while the api was idle.
	check := p.Interval
	if p.Idle != nil && check > time.Minute {
		check = time.Minute
	}
	every := int(p.Interval / check) // ticks between purges
	t := time.NewTicker(check)
	defer t.Stop()
	for ticks := every; ; ticks++ {
		if ticks >= every && (p.Idle == nil || !p.Idle()) {
			ticks = 0
			if n, err := p.Once(ctx, time.Now()); err != nil && ctx.Err() == nil {
				p.Logger.Warn("purge failed", "err", err)
			} else if n > 0 {
				p.Logger.Info("purged finished jobs", "count", n, "older_than", p.Keep)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Once deletes everything finished before now-Keep, in batches so no single
// statement holds locks for long. It returns how many jobs were deleted.
func (p *Purger) Once(ctx context.Context, now time.Time) (int64, error) {
	batch := p.Batch
	if batch <= 0 {
		batch = 5000
	}
	cutoff := now.Add(-p.Keep)
	var total int64
	for {
		n, err := p.Store.PurgeFinished(ctx, cutoff, batch)
		total += n
		if err != nil {
			return total, err
		}
		if n < int64(batch) {
			break
		}
	}
	_, err := p.Queue.TrimDead(ctx, cutoff)
	return total, err
}
