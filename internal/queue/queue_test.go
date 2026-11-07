package queue

import (
	"context"
	"testing"
	"time"

	"github.com/thelog5/relay/internal/testutil"
)

func newQueue(t *testing.T) *Queue {
	t.Helper()
	url, prefix := testutil.RedisURL(t)
	ctx := context.Background()
	rdb, err := Connect(ctx, url, 20)
	if err != nil {
		t.Fatal(err)
	}
	q := New(rdb, prefix)
	t.Cleanup(func() {
		keys, _ := rdb.Keys(context.Background(), prefix+":*").Result()
		if len(keys) > 0 {
			rdb.Del(context.Background(), keys...)
		}
		rdb.Close()
	})
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup must be idempotent: %v", err)
	}
	return q
}

func TestEnqueueReadAck(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	msgs, err := q.Read(ctx, "w1", 10, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].JobID != "job-1" {
		t.Fatalf("got %+v", msgs)
	}
	d, _ := q.Depth(ctx)
	if d.Stream != 1 || d.Pending != 1 || d.Backlog != 0 {
		t.Fatalf("depth while running: %+v", d)
	}
	// Nothing new for another consumer.
	if more, _ := q.Read(ctx, "w2", 10, 50*time.Millisecond); len(more) != 0 {
		t.Fatalf("message delivered twice: %+v", more)
	}
	if err := q.Ack(ctx, msgs[0].ID); err != nil {
		t.Fatal(err)
	}
	d, _ = q.Depth(ctx)
	if d.Stream != 0 || d.Pending != 0 {
		t.Fatalf("depth after ack: %+v", d)
	}
}

func TestReadOwnPendingAfterRestart(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	q.Enqueue(ctx, "job-1")
	if msgs, _ := q.Read(ctx, "w1", 10, 50*time.Millisecond); len(msgs) != 1 {
		t.Fatal("expected one message")
	}
	// Same consumer name comes back after a crash: its PEL is still there.
	msgs, err := q.ReadOwnPending(ctx, "w1", 10)
	if err != nil || len(msgs) != 1 || msgs[0].JobID != "job-1" {
		t.Fatalf("own pending: %v %+v", err, msgs)
	}
}

func TestRetryRoundTrip(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	q.Enqueue(ctx, "job-r")
	msgs, _ := q.Read(ctx, "w1", 1, 50*time.Millisecond)
	runAt := time.Now().Add(200 * time.Millisecond)
	if err := q.ScheduleRetry(ctx, msgs[0], runAt); err != nil {
		t.Fatal(err)
	}
	d, _ := q.Depth(ctx)
	if d.Stream != 0 || d.Pending != 0 || d.Retry != 1 {
		t.Fatalf("after schedule: %+v", d)
	}
	if due, _ := q.DueRetries(ctx, time.Now(), 100); len(due) != 0 {
		t.Fatalf("not due yet but got %v", due)
	}
	due, err := q.DueRetries(ctx, runAt.Add(time.Millisecond), 100)
	if err != nil || len(due) != 1 || due[0] != "job-r" {
		t.Fatalf("due: %v %v", err, due)
	}
	n, err := q.PromoteRetries(ctx, due)
	if err != nil || n != 1 {
		t.Fatalf("promote: %v %d", err, n)
	}
	// A second scheduler racing on the same IDs must not double-enqueue.
	if n, _ := q.PromoteRetries(ctx, due); n != 0 {
		t.Fatalf("double promote moved %d", n)
	}
	d, _ = q.Depth(ctx)
	if d.Stream != 1 || d.Retry != 0 {
		t.Fatalf("after promote: %+v", d)
	}
}

func TestDeadLetter(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	q.Enqueue(ctx, "job-d")
	msgs, _ := q.Read(ctx, "w1", 1, 50*time.Millisecond)
	if err := q.DeadLetter(ctx, msgs[0], "too many failures"); err != nil {
		t.Fatal(err)
	}
	d, _ := q.Depth(ctx)
	if d.Stream != 0 || d.Pending != 0 || d.Dead != 1 {
		t.Fatalf("after dead-letter: %+v", d)
	}
	entries, _ := q.Client().XRange(ctx, q.Keys.Dead, "-", "+").Result()
	if entries[0].Values["error"] != "too many failures" {
		t.Fatalf("last error not recorded: %+v", entries[0].Values)
	}
	n, err := q.RemoveDead(ctx, "job-d")
	if err != nil || n != 1 {
		t.Fatalf("remove dead: %v %d", err, n)
	}
}

func TestAutoClaimTakesOverIdleMessages(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	q.Enqueue(ctx, "job-a")
	q.Enqueue(ctx, "job-b")
	if msgs, _ := q.Read(ctx, "crashed", 10, 50*time.Millisecond); len(msgs) != 2 {
		t.Fatal("expected two messages")
	}
	// Not idle long enough yet.
	if got, _ := q.AutoClaim(ctx, "survivor", time.Second, 10); len(got) != 0 {
		t.Fatalf("claimed too early: %+v", got)
	}
	time.Sleep(150 * time.Millisecond)
	got, err := q.AutoClaim(ctx, "survivor", 100*time.Millisecond, 10)
	if err != nil || len(got) != 2 {
		t.Fatalf("autoclaim: %v %+v", err, got)
	}
	byC, _ := q.PendingByConsumer(ctx)
	if byC["survivor"] != 2 || byC["crashed"] != 0 {
		t.Fatalf("ownership not transferred: %+v", byC)
	}
	if n, _ := q.PruneConsumers(ctx, 0); n != 1 {
		t.Fatalf("expected the crashed consumer to be pruned, pruned %d", n)
	}
}

func TestExtendLeaseOnlyForOwner(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	q.Enqueue(ctx, "job-l")
	msgs, _ := q.Read(ctx, "owner", 1, 50*time.Millisecond)
	time.Sleep(120 * time.Millisecond)

	ok, err := q.ExtendLease(ctx, "owner", msgs[0].ID)
	if err != nil || !ok {
		t.Fatalf("owner extend: %v %v", ok, err)
	}
	// Lease was just renewed, so a 100ms-idle autoclaim finds nothing.
	if got, _ := q.AutoClaim(ctx, "thief", 100*time.Millisecond, 10); len(got) != 0 {
		t.Fatalf("heartbeated message was stolen: %+v", got)
	}
	ok, err = q.ExtendLease(ctx, "thief", msgs[0].ID)
	if err != nil || ok {
		t.Fatalf("non-owner must not extend: %v %v", ok, err)
	}
}

func TestKillControlMessage(t *testing.T) {
	q := newQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := q.SubscribeControl(ctx, "w-target")
	if err != nil {
		t.Fatal(err)
	}
	q.PublishKill(ctx, "w-other")
	n, err := q.PublishKill(ctx, "w-target")
	if err != nil || n < 1 {
		t.Fatalf("publish: %v %d", err, n)
	}
	select {
	case m := <-ch:
		if m.Action != "kill" || m.WorkerID != "w-target" {
			t.Fatalf("unexpected %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("kill not received")
	}
	select {
	case m := <-ch:
		t.Fatalf("received message for another worker: %+v", m)
	default:
	}
}

func TestEventsPubSub(t *testing.T) {
	q := newQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := q.SubscribeEvents(ctx)
	// Subscriptions are async; publish until one lands.
	deadline := time.After(2 * time.Second)
	for {
		q.PublishEvent(ctx, StatusEvent{JobID: "j", To: "running", At: time.Now()})
		select {
		case ev := <-ch:
			if ev.JobID != "j" || ev.To != "running" {
				t.Fatalf("bad event %+v", ev)
			}
			return
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatal("no event received")
		}
	}
}

func TestWorkerRegistry(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	start := time.Now()
	q.Heartbeat(ctx, WorkerInfo{ID: "w1", Host: "h1", PID: 11, Concurrency: 8, StartedAt: start})
	q.Heartbeat(ctx, WorkerInfo{ID: "w2", Host: "h2", PID: 22, Concurrency: 8, StartedAt: start.Add(time.Second)})
	q.IncrWorkerStat(ctx, "w1", StatSucceeded, 3)
	q.IncrWorkerStat(ctx, "w1", StatInFlight, 2)

	ws, err := q.Workers(ctx, 5*time.Second, time.Minute)
	if err != nil || len(ws) != 2 {
		t.Fatalf("workers: %v %+v", err, ws)
	}
	if ws[0].ID != "w1" || !ws[0].Alive || ws[0].Succeeded != 3 || ws[0].InFlight != 2 || ws[0].Concurrency != 8 {
		t.Fatalf("w1: %+v", ws[0])
	}
	time.Sleep(30 * time.Millisecond)
	ws, _ = q.Workers(ctx, 10*time.Millisecond, time.Minute)
	if ws[0].Alive || ws[0].InFlight != 0 {
		t.Fatalf("stale worker should be not alive with zero in-flight: %+v", ws[0])
	}
}
