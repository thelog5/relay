package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/thelog5/relay/internal/jobs"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/scheduler"
	"github.com/thelog5/relay/internal/store"
	"github.com/thelog5/relay/internal/testutil"
)

type harness struct {
	t     *testing.T
	store *store.Store
	q     *queue.Queue
	ctx   context.Context
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dsn := testutil.PostgresDSN(t)
	redisURL, prefix := testutil.RedisURL(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	st, err := store.Open(ctx, dsn, 20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	rdb, err := queue.Connect(ctx, redisURL, 50)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		keys, _ := rdb.Keys(context.Background(), prefix+":*").Result()
		if len(keys) > 0 {
			rdb.Del(context.Background(), keys...)
		}
		rdb.Close()
	})
	q := queue.New(rdb, prefix)
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, store: st, q: q, ctx: ctx}
}

// fastConfig shrinks every PRD timer so tests run in seconds.
func fastConfig(id string) Config {
	return Config{
		ID:                id,
		Concurrency:       8,
		ReclaimInterval:   200 * time.Millisecond,
		ReclaimIdle:       600 * time.Millisecond,
		LeaseInterval:     150 * time.Millisecond,
		HeartbeatInterval: 200 * time.Millisecond,
		ReadBlock:         200 * time.Millisecond,
		JobTimeout:        5 * time.Second,
		ShutdownGrace:     2 * time.Second,
		Backoff:           jobs.Backoff{Base: 10 * time.Millisecond, Max: 50 * time.Millisecond},
		Logger:            slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}
}

func (h *harness) startWorker(cfg Config) (*Worker, context.CancelFunc) {
	ctx, cancel := context.WithCancel(h.ctx)
	w := New(cfg, h.store, h.q, jobs.DefaultRegistry())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	h.t.Cleanup(func() { cancel(); <-done })
	return w, cancel
}

func (h *harness) startScheduler() {
	s := &scheduler.Scheduler{Store: h.store, Queue: h.q, Interval: 20 * time.Millisecond}
	go s.Run(h.ctx)
}

func (h *harness) enqueue(typ, payload string, maxAttempts int) string {
	h.t.Helper()
	id := uuid.NewString()
	if _, err := h.store.CreateJob(h.ctx, id, typ, json.RawMessage(payload), maxAttempts); err != nil {
		h.t.Fatal(err)
	}
	if _, err := h.q.Enqueue(h.ctx, id); err != nil {
		h.t.Fatal(err)
	}
	return id
}

// waitTerminal waits until n jobs are succeeded or dead.
func (h *harness) waitTerminal(n int64, timeout time.Duration) map[jobs.Status]int64 {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		c, err := h.store.CountByStatus(h.ctx)
		if err != nil {
			h.t.Fatal(err)
		}
		if c[jobs.StatusSucceeded]+c[jobs.StatusDead] >= n {
			return c
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %d terminal jobs; counts=%v", n, c)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (h *harness) assertCleanQueue() {
	h.t.Helper()
	d, err := h.q.Depth(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	if d.Stream != 0 || d.Pending != 0 || d.Retry != 0 {
		h.t.Fatalf("queue not drained: %+v", d)
	}
}

func TestSleepJobsSucceed(t *testing.T) {
	h := newHarness(t)
	h.startWorker(fastConfig("w1"))
	for i := 0; i < 50; i++ {
		h.enqueue("sleep", `{"ms":1}`, 5)
	}
	c := h.waitTerminal(50, 10*time.Second)
	if c[jobs.StatusSucceeded] != 50 {
		t.Fatalf("counts: %v", c)
	}
	h.assertCleanQueue()
}

func TestFlakyJobsRetryThenSettleWithNoneStuck(t *testing.T) {
	h := newHarness(t)
	h.startScheduler()
	h.startWorker(fastConfig("w1"))
	h.startWorker(fastConfig("w2"))

	const n = 300
	for i := 0; i < n; i++ {
		h.enqueue("flaky", `{"fail_rate":0.3}`, 3)
	}
	c := h.waitTerminal(n, 30*time.Second)
	if c[jobs.StatusQueued]+c[jobs.StatusRunning]+c[jobs.StatusRetryScheduled] != 0 {
		t.Fatalf("stuck jobs: %v", c)
	}
	// 0.3^3 * 300 = 8.1 expected dead; allow a wide band.
	if c[jobs.StatusDead] > 30 {
		t.Fatalf("too many dead jobs for a 30%% fail rate: %v", c)
	}
	h.assertCleanQueue()

	d, _ := h.q.Depth(h.ctx)
	if d.Dead != c[jobs.StatusDead] {
		t.Fatalf("dead-letter stream has %d, postgres has %d", d.Dead, c[jobs.StatusDead])
	}
	dead, _ := h.store.ListJobs(h.ctx, store.ListFilter{Status: jobs.StatusDead, Limit: 500})
	for _, j := range dead {
		if j.Attempts != 3 || j.LastError == "" {
			t.Fatalf("dead job without full attempts or error: %+v", j)
		}
	}
	// Every history must be a valid chain of the lifecycle.
	all, _ := h.store.ListJobs(h.ctx, store.ListFilter{Limit: 500})
	for _, j := range all {
		evs, _ := h.store.JobEvents(h.ctx, j.ID)
		for i, e := range evs {
			if i > 0 && e.FromStatus != evs[i-1].ToStatus {
				t.Fatalf("job %s broken history at %d: %+v", j.ID, i, evs)
			}
			if !jobs.CanTransition(e.FromStatus, e.ToStatus) {
				t.Fatalf("job %s illegal transition %q -> %q", j.ID, e.FromStatus, e.ToStatus)
			}
		}
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	h := newHarness(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	h.startScheduler()
	h.startWorker(fastConfig("w1"))
	id := h.enqueue("webhook", `{"url":"`+srv.URL+`"}`, 5)
	h.waitTerminal(1, 10*time.Second)
	j, _ := h.store.GetJob(h.ctx, id)
	if j.Status != jobs.StatusDead || j.Attempts != 1 {
		t.Fatalf("expected dead after 1 attempt: %+v", j)
	}
}

func TestWebhookRetriesSendSameIdempotencyKey(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	keys := make(chan string, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys <- r.Header.Get("Idempotency-Key")
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	h.startScheduler()
	h.startWorker(fastConfig("w1"))
	id := h.enqueue("webhook", `{"url":"`+srv.URL+`","body":{"n":1}}`, 5)
	h.waitTerminal(1, 10*time.Second)
	j, _ := h.store.GetJob(h.ctx, id)
	if j.Status != jobs.StatusSucceeded || j.Attempts != 3 {
		t.Fatalf("expected success on 3rd attempt: %+v", j)
	}
	close(keys)
	for k := range keys {
		if k != id {
			t.Fatalf("idempotency key %q != job id %q", k, id)
		}
	}
}

// A consumer reads jobs, marks them running, and then "crashes" (never acks).
// A live worker must reclaim and finish every one of them.
func TestReclaimJobsFromCrashedConsumer(t *testing.T) {
	h := newHarness(t)
	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, h.enqueue("sleep", `{"ms":10}`, 5))
	}
	msgs, err := h.q.Read(h.ctx, "crashed-worker", 10, 100*time.Millisecond)
	if err != nil || len(msgs) != 5 {
		t.Fatalf("crashed worker read: %v %d", err, len(msgs))
	}
	for _, m := range msgs {
		if _, _, err := h.store.Transition(h.ctx, m.JobID, store.TransitionOpts{
			From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, WorkerID: "crashed-worker", IncAttempts: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	crashAt := time.Now()

	h.startWorker(fastConfig("survivor"))
	h.waitTerminal(5, 10*time.Second)
	recovered := time.Since(crashAt)

	for _, id := range ids {
		j, _ := h.store.GetJob(h.ctx, id)
		if j.Status != jobs.StatusSucceeded || j.WorkerID != "survivor" || j.Attempts != 2 {
			t.Fatalf("job not recovered by survivor: %+v", j)
		}
		evs, _ := h.store.JobEvents(h.ctx, id)
		found := false
		for _, e := range evs {
			if e.FromStatus == jobs.StatusRunning && e.ToStatus == jobs.StatusRunning && e.Error == "reclaimed from crashed-worker" {
				found = true
			}
		}
		if !found {
			t.Fatalf("no reclaim event for %s: %+v", id, evs)
		}
	}
	// ReclaimIdle 600ms + ReclaimInterval 200ms + work: well under 3s.
	if recovered > 3*time.Second {
		t.Fatalf("recovery took %v", recovered)
	}
	rs, _ := h.store.Recovery(h.ctx)
	if rs.Reclaimed != 5 {
		t.Fatalf("recovery stats: %+v", rs)
	}
	h.assertCleanQueue()
}

func TestLeaseStopsHealthyLongJobFromBeingStolen(t *testing.T) {
	h := newHarness(t)
	h.startWorker(fastConfig("w1"))
	h.startWorker(fastConfig("w2"))
	// Runs 2.5x longer than ReclaimIdle; without leases it would be reclaimed.
	id := h.enqueue("sleep", `{"ms":1500}`, 5)
	h.waitTerminal(1, 10*time.Second)
	j, _ := h.store.GetJob(h.ctx, id)
	if j.Attempts != 1 {
		evs, _ := h.store.JobEvents(h.ctx, id)
		t.Fatalf("long job was stolen; attempts=%d events=%+v", j.Attempts, evs)
	}
}

func TestDuplicateDeliveryOfFinishedJobIsAcked(t *testing.T) {
	h := newHarness(t)
	id := h.enqueue("sleep", `{"ms":0}`, 5)
	// Simulate: a worker finished the job in Postgres, then crashed before XACK.
	h.store.Transition(h.ctx, id, store.TransitionOpts{From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, WorkerID: "ghost", IncAttempts: true})
	h.store.Transition(h.ctx, id, store.TransitionOpts{From: []jobs.Status{jobs.StatusRunning}, To: jobs.StatusSucceeded, WorkerID: "ghost"})

	h.startWorker(fastConfig("w1"))
	deadline := time.Now().Add(5 * time.Second)
	for {
		d, _ := h.q.Depth(h.ctx)
		if d.Stream == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("duplicate never acked: %+v", d)
		}
		time.Sleep(20 * time.Millisecond)
	}
	j, _ := h.store.GetJob(h.ctx, id)
	if j.Attempts != 1 || j.WorkerID != "ghost" {
		t.Fatalf("finished job was re-run: %+v", j)
	}
}

func TestCrashDuringFinalAttemptGoesDead(t *testing.T) {
	h := newHarness(t)
	id := h.enqueue("sleep", `{"ms":0}`, 1)
	msgs, _ := h.q.Read(h.ctx, "crashed", 1, 100*time.Millisecond)
	if len(msgs) != 1 {
		t.Fatal("expected one message")
	}
	h.store.Transition(h.ctx, id, store.TransitionOpts{From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, WorkerID: "crashed", IncAttempts: true})

	h.startWorker(fastConfig("w1"))
	h.waitTerminal(1, 10*time.Second)
	j, _ := h.store.GetJob(h.ctx, id)
	if j.Status != jobs.StatusDead {
		t.Fatalf("expected dead, got %+v", j)
	}
	h.assertCleanQueue()
}

func TestKillCommandExitsWithStatusOne(t *testing.T) {
	h := newHarness(t)
	codes := make(chan int, 1)
	cfg := fastConfig("doomed")
	cfg.Exit = func(code int) { codes <- code }
	h.startWorker(cfg)

	deadline := time.After(5 * time.Second)
	for {
		n, err := h.q.PublishKill(h.ctx, "doomed")
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("worker never subscribed")
		case <-time.After(20 * time.Millisecond):
		}
	}
	select {
	case code := <-codes:
		if code != 1 {
			t.Fatalf("exit code %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not exit")
	}
}

func TestWorkerRegistersAndCounts(t *testing.T) {
	h := newHarness(t)
	h.startWorker(fastConfig("counted"))
	for i := 0; i < 10; i++ {
		h.enqueue("sleep", `{"ms":0}`, 5)
	}
	h.waitTerminal(10, 10*time.Second)
	time.Sleep(50 * time.Millisecond)
	ws, err := h.q.Workers(h.ctx, 5*time.Second, time.Minute)
	if err != nil || len(ws) != 1 {
		t.Fatalf("workers: %v %+v", err, ws)
	}
	if ws[0].ID != "counted" || !ws[0].Alive || ws[0].Succeeded != 10 || ws[0].InFlight != 0 {
		t.Fatalf("worker stats: %+v", ws[0])
	}
}

// A registered worker stops heartbeating mid-job. Survivors must take its
// jobs via the fast path long before the XAUTOCLAIM idle threshold.
func TestFastRescueFromWorkerWithStaleHeartbeat(t *testing.T) {
	h := newHarness(t)
	var ids []string
	for i := 0; i < 4; i++ {
		ids = append(ids, h.enqueue("sleep", `{"ms":10}`, 5))
	}
	h.q.Heartbeat(h.ctx, queue.WorkerInfo{ID: "victim", StartedAt: time.Now()})
	msgs, _ := h.q.Read(h.ctx, "victim", 10, 100*time.Millisecond)
	if len(msgs) != 4 {
		t.Fatalf("victim read %d", len(msgs))
	}
	for _, m := range msgs {
		h.store.Transition(h.ctx, m.JobID, store.TransitionOpts{From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, WorkerID: "victim", IncAttempts: true})
	}
	killedAt := time.Now()

	cfg := fastConfig("rescuer")
	cfg.ReclaimIdle = time.Minute // make sure only the fast path can help
	cfg.ReclaimInterval = time.Minute
	h.startWorker(cfg)
	h.waitTerminal(4, 10*time.Second)
	took := time.Since(killedAt)
	// DeadAfter = 3 x 200ms heartbeat; minIdle = 2 x 150ms lease.
	if took > 3*time.Second {
		t.Fatalf("fast rescue took %v", took)
	}
	for _, id := range ids {
		j, _ := h.store.GetJob(h.ctx, id)
		if j.Status != jobs.StatusSucceeded || j.WorkerID != "rescuer" {
			t.Fatalf("job not rescued: %+v", j)
		}
	}
}

// Orphaned jobs must not wait behind a backlog of new work.
func TestRescuedJobsJumpTheBacklog(t *testing.T) {
	h := newHarness(t)
	var orphans []string
	for i := 0; i < 4; i++ {
		orphans = append(orphans, h.enqueue("sleep", `{"ms":10}`, 5))
	}
	h.q.Heartbeat(h.ctx, queue.WorkerInfo{ID: "victim", StartedAt: time.Now()})
	if msgs, _ := h.q.Read(h.ctx, "victim", 10, 100*time.Millisecond); len(msgs) != 4 {
		t.Fatal("victim should hold the orphans")
	}
	for _, id := range orphans {
		h.store.Transition(h.ctx, id, store.TransitionOpts{From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, WorkerID: "victim", IncAttempts: true})
	}
	// 4 slots x 100ms jobs: the backlog alone takes ~2.5s to drain.
	for i := 0; i < 100; i++ {
		h.enqueue("sleep", `{"ms":100}`, 5)
	}
	start := time.Now()
	cfg := fastConfig("rescuer")
	cfg.Concurrency = 4
	cfg.ReclaimIdle = time.Minute
	cfg.ReclaimInterval = time.Minute
	h.startWorker(cfg)

	deadline := time.Now().Add(10 * time.Second)
	for _, id := range orphans {
		for {
			j, _ := h.store.GetJob(h.ctx, id)
			if j.Status == jobs.StatusSucceeded {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("orphan %s never finished", id)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if took := time.Since(start); took > 1500*time.Millisecond {
		t.Fatalf("orphans waited behind the backlog: %v", took)
	}
	h.waitTerminal(104, 20*time.Second)
}
