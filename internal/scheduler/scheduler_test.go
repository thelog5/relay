package scheduler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/thelog5/relay/internal/jobs"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/store"
	"github.com/thelog5/relay/internal/testutil"
)

func setup(t *testing.T) (*store.Store, *queue.Queue) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, testutil.PostgresDSN(t), 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	url, prefix := testutil.RedisURL(t)
	rdb, err := queue.Connect(ctx, url, 10)
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
	return st, q
}

func TestTickPromotesDueRetries(t *testing.T) {
	st, q := setup(t)
	ctx := context.Background()
	id := uuid.NewString()
	st.CreateJob(ctx, id, "sleep", json.RawMessage(`{"ms":1}`), 5)
	q.Enqueue(ctx, id)
	msgs, _ := q.Read(ctx, "w", 1, 50*time.Millisecond)
	st.Transition(ctx, id, store.TransitionOpts{From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, IncAttempts: true})
	st.Transition(ctx, id, store.TransitionOpts{From: []jobs.Status{jobs.StatusRunning}, To: jobs.StatusRetryScheduled, Error: "x"})
	runAt := time.Now().Add(time.Second)
	q.ScheduleRetry(ctx, msgs[0], runAt)

	s := &Scheduler{Store: st, Queue: q, Batch: 100}
	if n, _ := s.Tick(ctx, time.Now()); n != 0 {
		t.Fatalf("promoted %d before due", n)
	}
	if n, err := s.Tick(ctx, runAt.Add(time.Millisecond)); err != nil || n != 1 {
		t.Fatalf("tick: %d %v", n, err)
	}
	j, _ := st.GetJob(ctx, id)
	if j.Status != jobs.StatusQueued {
		t.Fatalf("status %s", j.Status)
	}
	d, _ := q.Depth(ctx)
	if d.Retry != 0 || d.Backlog != 1 {
		t.Fatalf("depth %+v", d)
	}
}

func TestReconcileReenqueuesOnlyMissingJobs(t *testing.T) {
	st, q := setup(t)
	ctx := context.Background()
	// orphan: row written, XADD never happened (api crashed in between).
	orphan := uuid.NewString()
	st.CreateJob(ctx, orphan, "sleep", json.RawMessage(`{"ms":1}`), 5)
	// healthy: on the stream as usual.
	healthy := uuid.NewString()
	st.CreateJob(ctx, healthy, "sleep", json.RawMessage(`{"ms":1}`), 5)
	q.Enqueue(ctx, healthy)
	// parked: in the retry set while Postgres already says queued (the
	// scheduler's own write order).
	parked := uuid.NewString()
	st.CreateJob(ctx, parked, "sleep", json.RawMessage(`{"ms":1}`), 5)
	q.Client().ZAdd(ctx, q.Keys.Retry, redisZ(parked))

	r := &Reconciler{Store: st, Queue: q, Grace: time.Minute}
	if n, _ := r.Once(ctx, time.Now()); n != 0 {
		t.Fatalf("reconciled %d jobs still inside the grace period", n)
	}
	n, err := r.Once(ctx, time.Now().Add(2*time.Minute))
	if err != nil || n != 1 {
		t.Fatalf("reconcile: %d %v", n, err)
	}
	msgs, _ := q.Read(ctx, "w", 10, 50*time.Millisecond)
	seen := map[string]int{}
	for _, m := range msgs {
		seen[m.JobID]++
	}
	if seen[orphan] != 1 || seen[healthy] != 1 || len(msgs) != 2 {
		t.Fatalf("stream after reconcile: %v", seen)
	}
	// A second pass finds nothing new to do.
	if n, _ := r.Once(ctx, time.Now().Add(2*time.Minute)); n != 0 {
		t.Fatalf("second pass re-enqueued %d", n)
	}
}

// Redis restarting without persistence loses every stream entry and retry
// timer. Postgres still knows which jobs are unfinished; the reconciler puts
// exactly the ones Redis forgot back on the stream.
func TestReconcileRecoversJobsAfterRedisDataLoss(t *testing.T) {
	st, q := setup(t)
	ctx := context.Background()
	newJob := func() string {
		id := uuid.NewString()
		if _, err := st.CreateJob(ctx, id, "sleep", json.RawMessage(`{"ms":1}`), 5); err != nil {
			t.Fatal(err)
		}
		return id
	}
	toStatus := func(id string, from, to jobs.Status) {
		if _, _, err := st.Transition(ctx, id, store.TransitionOpts{From: []jobs.Status{from}, To: to, WorkerID: "w-old", IncAttempts: to == jobs.StatusRunning}); err != nil {
			t.Fatal(err)
		}
	}
	// Running on a live worker: its message is still pending in the stream.
	live := newJob()
	q.Enqueue(ctx, live)
	q.Read(ctx, "w-live", 10, 10*time.Millisecond)
	toStatus(live, jobs.StatusQueued, jobs.StatusRunning)
	// Waiting out a backoff in the retry set.
	waiting := newJob()
	toStatus(waiting, jobs.StatusQueued, jobs.StatusRunning)
	toStatus(waiting, jobs.StatusRunning, jobs.StatusRetryScheduled)
	q.Client().ZAdd(ctx, q.Keys.Retry, redisZ(waiting))
	// Lost with the Redis restart: Postgres says running / retrying, Redis
	// has nothing.
	lostRunning := newJob()
	toStatus(lostRunning, jobs.StatusQueued, jobs.StatusRunning)
	lostRetry := newJob()
	toStatus(lostRetry, jobs.StatusQueued, jobs.StatusRunning)
	toStatus(lostRetry, jobs.StatusRunning, jobs.StatusRetryScheduled)

	r := &Reconciler{Store: st, Queue: q, Grace: time.Minute}
	n, err := r.Once(ctx, time.Now().Add(2*time.Minute))
	if err != nil || n != 2 {
		t.Fatalf("reconcile: %d %v", n, err)
	}
	msgs, _ := q.Read(ctx, "w-new", 10, 50*time.Millisecond)
	got := map[string]bool{}
	for _, m := range msgs {
		got[m.JobID] = true
	}
	if len(msgs) != 2 || !got[lostRunning] || !got[lostRetry] {
		t.Fatalf("re-enqueued %v, want only the two lost jobs", got)
	}
}
