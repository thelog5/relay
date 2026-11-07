// Package worker runs jobs from the Redis stream (R3).
//
// A worker process runs Concurrency jobs at once. Each delivery goes through:
//
//  1. Postgres: mark running, attempts+1, worker_id = me (any non-final
//     status is accepted, because Redis decides what runs; a finished job is
//     a duplicate delivery and is acked and skipped).
//  2. Run the handler, extending the Redis lease every few seconds.
//  3. Postgres: record the outcome, fenced on worker_id = me so a worker that
//     lost the job to a reclaim cannot also finish it.
//  4. Redis: ack, park in the retry set, or dead-letter, atomically.
//
// If the process dies anywhere in there, the message stays in this
// consumer's pending list, goes idle, and a live worker's XAUTOCLAIM takes it
// over within ReclaimIdle + ReclaimInterval (R6). Nothing is ever lost; the
// cost is that a job can run twice (at-least-once).
package worker

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thelog5/relay/internal/jobs"
	"github.com/thelog5/relay/internal/metrics"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/store"
)

// Config tunes one worker process. Zero values get the PRD defaults.
type Config struct {
	ID          string
	Concurrency int
	// ReclaimInterval is how often XAUTOCLAIM runs (PRD: 5s).
	ReclaimInterval time.Duration
	// ReclaimIdle is how long a message must sit unacknowledged before
	// another worker may take it (PRD: 10s).
	ReclaimIdle time.Duration
	// LeaseInterval is how often a running job's idle timer is reset so it
	// is not reclaimed while healthy. 0 disables lease extension.
	LeaseInterval time.Duration
	// HeartbeatInterval is how often the worker refreshes its registry entry.
	HeartbeatInterval time.Duration
	// JobTimeout bounds a single handler run.
	JobTimeout time.Duration
	// ShutdownGrace is how long SIGTERM waits for in-flight jobs.
	ShutdownGrace time.Duration
	// ReadBlock is the XREADGROUP BLOCK timeout.
	ReadBlock time.Duration
	Backoff   jobs.Backoff
	// Exit is called on a kill command. Defaults to os.Exit.
	Exit   func(code int)
	Logger *slog.Logger
}

func (c *Config) defaults() {
	if c.ID == "" {
		c.ID = DefaultID()
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	if c.ReclaimInterval <= 0 {
		c.ReclaimInterval = 5 * time.Second
	}
	if c.ReclaimIdle <= 0 {
		c.ReclaimIdle = 10 * time.Second
	}
	if c.LeaseInterval < 0 {
		c.LeaseInterval = 0
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 2 * time.Second
	}
	if c.JobTimeout <= 0 {
		c.JobTimeout = 60 * time.Second
	}
	if c.ShutdownGrace <= 0 {
		c.ShutdownGrace = 20 * time.Second
	}
	if c.ReadBlock <= 0 {
		c.ReadBlock = 2 * time.Second
	}
	if c.Backoff.Base == 0 {
		c.Backoff = jobs.DefaultBackoff()
	}
	if c.Exit == nil {
		c.Exit = os.Exit
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// DefaultID is hostname plus a random suffix. The suffix matters: a worker
// container restarted by Docker keeps its hostname, and a fresh consumer name
// means its old pending messages are reclaimed like any crashed worker's.
func DefaultID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	b := make([]byte, 3)
	_, _ = crand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}

// Worker is one worker process's runtime.
type Worker struct {
	cfg   Config
	store *store.Store
	q     *queue.Queue
	reg   jobs.Registry
	log   *slog.Logger

	slots    chan struct{} // buffered to Concurrency; send = acquire
	wg       sync.WaitGroup
	inFlight atomic.Int64
	started  time.Time
}

// New builds a worker. Call Run to start it.
func New(cfg Config, st *store.Store, q *queue.Queue, reg jobs.Registry) *Worker {
	cfg.defaults()
	return &Worker{
		cfg:   cfg,
		store: st,
		q:     q,
		reg:   reg,
		log:   cfg.Logger.With("worker", cfg.ID),
		slots: make(chan struct{}, cfg.Concurrency),
	}
}

// ID returns the worker's consumer name.
func (w *Worker) ID() string { return w.cfg.ID }

// InFlight returns how many jobs are executing right now.
func (w *Worker) InFlight() int64 { return w.inFlight.Load() }

// Run processes jobs until ctx is cancelled, then waits up to ShutdownGrace
// for in-flight jobs before returning.
func (w *Worker) Run(ctx context.Context) error {
	w.started = time.Now()
	if err := w.q.EnsureGroup(ctx); err != nil {
		return fmt.Errorf("ensure consumer group: %w", err)
	}

	// Jobs get their own context so SIGTERM lets them finish; it is only
	// cancelled if the grace period runs out.
	jobsCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()

	control, err := w.q.SubscribeControl(ctx, w.cfg.ID)
	if err != nil {
		return fmt.Errorf("subscribe control: %w", err)
	}
	go w.watchControl(ctx, control)

	if err := w.heartbeat(ctx); err != nil {
		w.log.Warn("first heartbeat failed", "err", err)
	}
	go w.heartbeatLoop(ctx)
	go w.reclaimLoop(ctx, jobsCtx)

	w.log.Info("worker started",
		"concurrency", w.cfg.Concurrency,
		"reclaim_interval", w.cfg.ReclaimInterval,
		"reclaim_idle", w.cfg.ReclaimIdle,
		"lease_interval", w.cfg.LeaseInterval)

	w.fetchLoop(ctx, jobsCtx)

	w.log.Info("shutting down, waiting for in-flight jobs", "in_flight", w.inFlight.Load())
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
		w.log.Info("all jobs finished, bye")
	case <-time.After(w.cfg.ShutdownGrace):
		w.log.Warn("grace period over; abandoning in-flight jobs to be reclaimed", "in_flight", w.inFlight.Load())
		cancelJobs()
	}
	return nil
}

func (w *Worker) watchControl(ctx context.Context, ch <-chan queue.ControlMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			if m.Action == "kill" {
				// Demo only (POST /workers/:id/kill): die mid-job without
				// acking anything, exactly like a crash.
				w.log.Warn("kill command received, exiting with status 1", "in_flight", w.inFlight.Load())
				w.cfg.Exit(1)
				return
			}
		}
	}
}

func (w *Worker) heartbeat(ctx context.Context) error {
	host, _ := os.Hostname()
	if err := w.q.Heartbeat(ctx, queue.WorkerInfo{
		ID:          w.cfg.ID,
		Host:        host,
		PID:         os.Getpid(),
		Concurrency: w.cfg.Concurrency,
		StartedAt:   w.started,
	}); err != nil {
		return err
	}
	return w.q.SetWorkerStat(ctx, w.cfg.ID, queue.StatInFlight, w.inFlight.Load())
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.heartbeat(ctx); err != nil && ctx.Err() == nil {
				w.log.Warn("heartbeat failed", "err", err)
			}
		}
	}
}

// acquire takes between 1 and max slots, blocking for the first.
func (w *Worker) acquire(ctx context.Context, max int) int {
	select {
	case w.slots <- struct{}{}:
	case <-ctx.Done():
		return 0
	}
	n := 1
	for n < max {
		select {
		case w.slots <- struct{}{}:
			n++
		default:
			return n
		}
	}
	return n
}

// tryAcquire takes up to max slots without blocking.
func (w *Worker) tryAcquire(max int) int {
	n := 0
	for n < max {
		select {
		case w.slots <- struct{}{}:
			n++
		default:
			return n
		}
	}
	return n
}

func (w *Worker) release(n int) {
	for i := 0; i < n; i++ {
		<-w.slots
	}
}

func (w *Worker) fetchLoop(ctx, jobsCtx context.Context) {
	// Anything this consumer name was holding before a restart comes first.
	if n := w.tryAcquire(w.cfg.Concurrency); n > 0 {
		msgs, err := w.q.ReadOwnPending(ctx, w.cfg.ID, int64(n))
		if err != nil {
			w.log.Warn("read own pending failed", "err", err)
		}
		w.release(n - len(msgs))
		for _, m := range msgs {
			w.dispatch(jobsCtx, m, false)
		}
	}

	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		n := w.acquire(ctx, w.cfg.Concurrency)
		if n == 0 {
			return
		}
		msgs, err := w.q.Read(ctx, w.cfg.ID, int64(n), w.cfg.ReadBlock)
		w.release(n - len(msgs))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.log.Warn("read failed", "err", err, "retry_in", backoff)
			sleep(ctx, backoff)
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		for _, m := range msgs {
			w.dispatch(jobsCtx, m, false)
		}
	}
}

func (w *Worker) reclaimLoop(ctx, jobsCtx context.Context) {
	// Stagger workers so their XAUTOCLAIMs do not all land at once, which
	// also shortens the worst-case recovery time across the fleet.
	sleep(ctx, time.Duration(rand.Int64N(int64(w.cfg.ReclaimInterval))))
	t := time.NewTicker(w.cfg.ReclaimInterval)
	defer t.Stop()
	ticks := 0
	for {
		w.reclaimOnce(ctx, jobsCtx)
		ticks++
		if ticks%12 == 0 {
			if n, err := w.q.PruneConsumers(ctx, 5*time.Minute); err == nil && n > 0 {
				w.log.Info("pruned idle consumers", "count", n)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (w *Worker) reclaimOnce(ctx, jobsCtx context.Context) {
	// Claim only what we have capacity to run. Slot accounting uses jobs
	// actually executing, not slots: the fetch loop holds idle slots while
	// it blocks in XREADGROUP, and reclaim must not be starved by that.
	free := w.cfg.Concurrency - int(w.inFlight.Load())
	if free <= 0 {
		return // busy; a worker with spare capacity will take them
	}
	msgs, err := w.q.AutoClaim(ctx, w.cfg.ID, w.cfg.ReclaimIdle, int64(free))
	if err != nil {
		if ctx.Err() == nil {
			w.log.Warn("autoclaim failed", "err", err)
		}
		return
	}
	for _, m := range msgs {
		// Blocks at most one ReadBlock: Go hands a freed slot to the
		// longest-waiting sender, so the fetch loop cannot jump the line.
		if w.acquire(ctx, 1) == 0 {
			return // shutting down; the claimed messages stay pending
		}
		w.log.Info("reclaimed job from idle consumer", "job_id", m.JobID, "msg_id", m.ID)
		w.dispatch(jobsCtx, m, true)
	}
}

// dispatch runs one message on its own goroutine. The caller has already
// acquired a slot for it; the goroutine releases it.
func (w *Worker) dispatch(ctx context.Context, m queue.Message, reclaimed bool) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer w.release(1)
		w.process(ctx, m, reclaimed)
	}()
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// startable is every status a delivery may find the job in. Redis is the
// authority on what runs, so even retry_scheduled (a crash between the
// Postgres write and the Redis move) or running (a reclaim) is fine.
var startable = []jobs.Status{jobs.StatusQueued, jobs.StatusRunning, jobs.StatusRetryScheduled}

func (w *Worker) process(ctx context.Context, m queue.Message, reclaimed bool) {
	log := w.log.With("job_id", m.JobID, "msg_id", m.ID)
	if m.JobID == "" {
		log.Warn("message without job_id, dropping")
		_ = w.q.Ack(ctx, m.ID)
		return
	}

	job, prev, err := w.store.Transition(ctx, m.JobID, store.TransitionOpts{
		From: startable, To: jobs.StatusRunning, WorkerID: w.cfg.ID, IncAttempts: true,
	})
	var conflict *store.ConflictError
	switch {
	case errors.As(err, &conflict):
		// Already succeeded or dead: a duplicate delivery after a crash
		// between the Postgres write and the ack. Just ack it.
		metrics.DuplicateDeliveries.Inc()
		log.Info("job already finished, acking duplicate delivery", "status", conflict.Current)
		_ = w.q.Ack(ctx, m.ID)
		return
	case errors.Is(err, store.ErrNotFound):
		log.Warn("job row not found (deleted?), dropping message")
		_ = w.q.Ack(ctx, m.ID)
		return
	case err != nil:
		// Postgres unavailable: leave the message pending; it will be
		// reclaimed and retried once the database is back.
		log.Error("could not start job, leaving it pending", "err", err)
		return
	}

	if prev == jobs.StatusRunning {
		reclaimed = true
	}
	if reclaimed {
		metrics.JobsReclaimed.Inc()
		_ = w.q.IncrWorkerStat(ctx, w.cfg.ID, queue.StatReclaimed, 1)
	}
	w.publish(ctx, job, prev, "")

	// A worker crashed during this job's final attempt: do not run it again.
	// This also stops a job that kills its worker every time (a poison pill)
	// from crash-looping the fleet forever.
	if job.Attempts > job.MaxAttempts {
		reason := fmt.Sprintf("exceeded max attempts (%d): worker crashed during the final attempt", job.MaxAttempts)
		w.finish(ctx, m, job, jobs.StatusDead, reason, 0, metrics.OutcomeDead)
		return
	}

	w.inFlight.Add(1)
	metrics.JobsInFlight.Inc()
	_ = w.q.IncrWorkerStat(ctx, w.cfg.ID, queue.StatInFlight, 1)

	start := time.Now()
	runErr := w.run(ctx, m, job)
	elapsed := time.Since(start)

	w.inFlight.Add(-1)
	metrics.JobsInFlight.Dec()
	_ = w.q.IncrWorkerStat(context.Background(), w.cfg.ID, queue.StatInFlight, -1)

	if ctx.Err() != nil {
		// Forced shutdown: leave the message pending for another worker.
		log.Warn("job interrupted by shutdown; it will be reclaimed")
		return
	}

	switch {
	case runErr == nil:
		w.finish(ctx, m, job, jobs.StatusSucceeded, "", elapsed, metrics.OutcomeSucceeded)
	case jobs.IsPermanent(runErr) || job.Attempts >= job.MaxAttempts:
		w.finish(ctx, m, job, jobs.StatusDead, runErr.Error(), elapsed, metrics.OutcomeDead)
	default:
		w.finish(ctx, m, job, jobs.StatusRetryScheduled, runErr.Error(), elapsed, metrics.OutcomeRetried)
	}
}

// run executes the handler with a timeout, extending the lease meanwhile and
// turning panics into errors.
func (w *Worker) run(ctx context.Context, m queue.Message, job *jobs.Job) (err error) {
	h, err := w.reg.Lookup(job.Type)
	if err != nil {
		return jobs.Permanent(err)
	}
	jctx, cancel := context.WithTimeout(ctx, w.cfg.JobTimeout)
	defer cancel()

	if w.cfg.LeaseInterval > 0 {
		stop := make(chan struct{})
		defer close(stop)
		go w.extendLease(m, stop)
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	err = h.Handle(jctx, job)
	if err != nil && errors.Is(err, context.DeadlineExceeded) && jctx.Err() != nil && ctx.Err() == nil {
		err = fmt.Errorf("job timed out after %s", w.cfg.JobTimeout)
	}
	return err
}

func (w *Worker) extendLease(m queue.Message, stop <-chan struct{}) {
	t := time.NewTicker(w.cfg.LeaseInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			ok, err := w.q.ExtendLease(context.Background(), w.cfg.ID, m.ID)
			if err != nil {
				w.log.Warn("lease extend failed", "job_id", m.JobID, "err", err)
			} else if !ok {
				w.log.Warn("lost lease: job was reclaimed by another worker", "job_id", m.JobID)
				return
			}
		}
	}
}

// finish records the outcome in Postgres (fenced on ownership), then moves
// the message in Redis.
func (w *Worker) finish(ctx context.Context, m queue.Message, job *jobs.Job, to jobs.Status, errMsg string, elapsed time.Duration, outcome string) {
	log := w.log.With("job_id", job.ID, "attempt", job.Attempts)
	done, prev, err := w.store.Transition(ctx, job.ID, store.TransitionOpts{
		From: []jobs.Status{jobs.StatusRunning}, To: to, WorkerID: w.cfg.ID, Error: errMsg, OwnedBy: w.cfg.ID,
	})
	var conflict *store.ConflictError
	if errors.As(err, &conflict) {
		// Another worker reclaimed the job while we were running it. Its
		// result wins; do not touch the message, which it now owns.
		metrics.LeaseLost.Inc()
		log.Warn("discarding result: job now belongs to another worker", "status", conflict.Current)
		return
	}
	if err != nil {
		log.Error("could not record outcome; message stays pending and will be retried", "err", err)
		return
	}

	switch to {
	case jobs.StatusSucceeded:
		err = w.q.Ack(ctx, m.ID)
		_ = w.q.IncrWorkerStat(ctx, w.cfg.ID, queue.StatSucceeded, 1)
	case jobs.StatusRetryScheduled:
		delay := w.cfg.Backoff.Delay(job.Attempts)
		err = w.q.ScheduleRetry(ctx, m, time.Now().Add(delay))
		_ = w.q.IncrWorkerStat(ctx, w.cfg.ID, queue.StatFailed, 1)
		log.Info("job failed, retry scheduled", "err", errMsg, "retry_in", delay.Round(time.Millisecond))
	case jobs.StatusDead:
		err = w.q.DeadLetter(ctx, m, errMsg)
		_ = w.q.IncrWorkerStat(ctx, w.cfg.ID, queue.StatDead, 1)
		log.Warn("job dead-lettered", "err", errMsg)
	}
	if err != nil {
		// Postgres already has the outcome. The message stays pending; when
		// it is reclaimed the next worker sees the final status and acks it,
		// or (for retries) simply runs it again.
		log.Error("redis update after outcome failed", "to", to, "err", err)
	}

	metrics.JobsCompleted.WithLabelValues(job.Type, outcome).Inc()
	if elapsed > 0 {
		metrics.JobDuration.WithLabelValues(job.Type, outcome).Observe(elapsed.Seconds())
	}
	w.publish(ctx, done, prev, errMsg)
}

func (w *Worker) publish(ctx context.Context, j *jobs.Job, from jobs.Status, errMsg string) {
	_ = w.q.PublishEvent(ctx, queue.StatusEvent{
		JobID:    j.ID,
		Type:     j.Type,
		From:     string(from),
		To:       string(j.Status),
		WorkerID: w.cfg.ID,
		Error:    errMsg,
		Attempts: j.Attempts,
		At:       j.UpdatedAt,
	})
}
