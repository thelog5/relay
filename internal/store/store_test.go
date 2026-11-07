package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/thelog5/relay/internal/jobs"
	"github.com/thelog5/relay/internal/testutil"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	s, err := Open(ctx, testutil.PostgresDSN(t), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// Running twice must be a no-op.
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	return s
}

func create(t *testing.T, s *Store) *jobs.Job {
	t.Helper()
	j, err := s.CreateJob(context.Background(), uuid.NewString(), "sleep", json.RawMessage(`{"ms":1}`), 3)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestCreateAndGet(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	j := create(t, s)
	if j.Status != jobs.StatusQueued || j.Attempts != 0 || j.MaxAttempts != 3 {
		t.Fatalf("unexpected new job: %+v", j)
	}
	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != j.ID || string(got.Payload) != `{"ms": 1}` {
		t.Fatalf("got %+v payload %s", got, got.Payload)
	}
	evs, err := s.JobEvents(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].FromStatus != "" || evs[0].ToStatus != jobs.StatusQueued {
		t.Fatalf("unexpected events: %+v", evs)
	}
	if _, err := s.GetJob(ctx, uuid.NewString()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestFullLifecycleWritesEveryEvent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	j := create(t, s)

	steps := []TransitionOpts{
		{From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, WorkerID: "w1", IncAttempts: true},
		{From: []jobs.Status{jobs.StatusRunning}, To: jobs.StatusRetryScheduled, WorkerID: "w1", Error: "boom"},
		{From: []jobs.Status{jobs.StatusRetryScheduled}, To: jobs.StatusQueued, WorkerID: "scheduler"},
		{From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, WorkerID: "w2", IncAttempts: true},
		{From: []jobs.Status{jobs.StatusRunning}, To: jobs.StatusSucceeded, WorkerID: "w2"},
	}
	for i, st := range steps {
		if _, _, err := s.Transition(ctx, j.ID, st); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.Status != jobs.StatusSucceeded || got.Attempts != 2 || got.LastError != "boom" || got.WorkerID != "w2" {
		t.Fatalf("final job wrong: %+v", got)
	}
	evs, _ := s.JobEvents(ctx, j.ID)
	if len(evs) != 6 {
		t.Fatalf("want 6 events, got %d: %+v", len(evs), evs)
	}
	want := []jobs.Status{jobs.StatusQueued, jobs.StatusRunning, jobs.StatusRetryScheduled, jobs.StatusQueued, jobs.StatusRunning, jobs.StatusSucceeded}
	for i, e := range evs {
		if e.ToStatus != want[i] {
			t.Errorf("event %d: to=%s want %s", i, e.ToStatus, want[i])
		}
		if i > 0 && e.FromStatus != evs[i-1].ToStatus {
			t.Errorf("event %d: from=%s, previous to=%s", i, e.FromStatus, evs[i-1].ToStatus)
		}
		if e.At.IsZero() {
			t.Errorf("event %d has no timestamp", i)
		}
	}
	if evs[1].WorkerID != "w1" || evs[2].Error != "boom" {
		t.Fatalf("worker/error not recorded: %+v", evs)
	}
}

func TestTransitionConflict(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	j := create(t, s)
	_, _, err := s.Transition(ctx, j.ID, TransitionOpts{From: []jobs.Status{jobs.StatusRunning}, To: jobs.StatusSucceeded})
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Current != jobs.StatusQueued {
		t.Fatalf("expected conflict with current=queued, got %v", err)
	}
	evs, _ := s.JobEvents(ctx, j.ID)
	if len(evs) != 1 {
		t.Fatalf("conflicting transition must not write an event, got %d", len(evs))
	}
}

func TestConcurrentTransitionsOnlyOneWins(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	j := create(t, s)

	const n = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := s.Transition(ctx, j.ID, TransitionOpts{
				From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, WorkerID: "racer", IncAttempts: true,
			})
			if err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("expected exactly one winner, got %d", wins)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.Attempts != 1 {
		t.Fatalf("attempts should be 1, got %d", got.Attempts)
	}
}

func TestReclaimRecordsRunningToRunning(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	j := create(t, s)
	running := []jobs.Status{jobs.StatusQueued, jobs.StatusRunning}
	if _, _, err := s.Transition(ctx, j.ID, TransitionOpts{From: running, To: jobs.StatusRunning, WorkerID: "dead-worker", IncAttempts: true}); err != nil {
		t.Fatal(err)
	}
	_, prev, err := s.Transition(ctx, j.ID, TransitionOpts{From: running, To: jobs.StatusRunning, WorkerID: "live-worker", IncAttempts: true, Error: "reclaimed from dead-worker"})
	if err != nil {
		t.Fatal(err)
	}
	if prev != jobs.StatusRunning {
		t.Fatalf("prev status = %s", prev)
	}
	rs, err := s.Recovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Reclaimed != 1 {
		t.Fatalf("expected 1 reclaim, got %+v", rs)
	}
}

func TestRequeueDueAndCounts(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	a, b, c := create(t, s), create(t, s), create(t, s)
	for _, j := range []*jobs.Job{a, b} {
		s.Transition(ctx, j.ID, TransitionOpts{From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, IncAttempts: true})
		s.Transition(ctx, j.ID, TransitionOpts{From: []jobs.Status{jobs.StatusRunning}, To: jobs.StatusRetryScheduled, Error: "x"})
	}
	counts, err := s.CountByStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts[jobs.StatusRetryScheduled] != 2 || counts[jobs.StatusQueued] != 1 || counts[jobs.StatusDead] != 0 {
		t.Fatalf("counts: %+v", counts)
	}
	moved, err := s.RequeueDue(ctx, []string{a.ID, b.ID, c.ID}, "scheduler")
	if err != nil {
		t.Fatal(err)
	}
	if len(moved) != 2 {
		t.Fatalf("expected 2 moved (c was already queued), got %v", moved)
	}
	counts, _ = s.CountByStatus(ctx)
	if counts[jobs.StatusQueued] != 3 {
		t.Fatalf("counts after requeue: %+v", counts)
	}
}

func TestListAndDelete(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	j := create(t, s)
	create(t, s)
	s.Transition(ctx, j.ID, TransitionOpts{From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, IncAttempts: true})
	s.Transition(ctx, j.ID, TransitionOpts{From: []jobs.Status{jobs.StatusRunning}, To: jobs.StatusDead, Error: "gave up"})

	dead, err := s.ListJobs(ctx, ListFilter{Status: jobs.StatusDead})
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].ID != j.ID || dead[0].LastError != "gave up" {
		t.Fatalf("dead list: %+v", dead)
	}
	all, _ := s.ListJobs(ctx, ListFilter{})
	if len(all) != 2 {
		t.Fatalf("all list: %d", len(all))
	}
	feed, err := s.RecentEvents(ctx, 10)
	if err != nil || len(feed) != 4 || feed[0].Type != "sleep" {
		t.Fatalf("feed: %v %+v", err, feed)
	}

	queued := all[0]
	if queued.ID == j.ID {
		queued = all[1]
	}
	var ce *ConflictError
	if err := s.DeleteJob(ctx, queued.ID); !errors.As(err, &ce) {
		t.Fatalf("deleting a queued job should conflict, got %v", err)
	}
	if err := s.DeleteJob(ctx, j.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetJob(ctx, j.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("job should be gone")
	}
}
