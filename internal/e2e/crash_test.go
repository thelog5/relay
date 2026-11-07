// Package e2e crashes real worker processes. It builds cmd/worker, runs
// several copies against a real Postgres and Redis with the PRD's timers,
// and kills them with the same POST /workers/:id/kill a dashboard user would
// click (the worker calls os.Exit(1) mid-job) or with SIGKILL.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/thelog5/relay/internal/api"
	"github.com/thelog5/relay/internal/jobs"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/scheduler"
	"github.com/thelog5/relay/internal/store"
	"github.com/thelog5/relay/internal/testutil"
)

var (
	buildOnce sync.Once
	workerBin string
	buildErr  error
)

func buildWorker(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "relay-e2e")
		if err != nil {
			buildErr = err
			return
		}
		workerBin = filepath.Join(dir, "worker")
		_, file, _, _ := runtime.Caller(0)
		root := filepath.Join(filepath.Dir(file), "..", "..")
		cmd := exec.Command("go", "build", "-o", workerBin, "./cmd/worker")
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("build worker: %w\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return workerBin
}

type cluster struct {
	t        *testing.T
	dsn      string
	redisURL string
	prefix   string
	store    *store.Store
	q        *queue.Queue
	api      *httptest.Server
	ctx      context.Context

	mu    sync.Mutex
	procs map[string]*exec.Cmd
	exits map[string]chan error
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	if testing.Short() {
		t.Skip("e2e crash tests are slow; skipped in -short mode")
	}
	dsn := testutil.PostgresDSN(t)
	redisURL, prefix := testutil.RedisURL(t)
	buildWorker(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	st, err := store.Open(ctx, dsn, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	rdb, err := queue.Connect(ctx, redisURL, 20)
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
	quiet := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv := api.New(api.Config{Logger: quiet}, st, q, jobs.DefaultRegistry())
	go srv.RunHub(ctx)
	go (&scheduler.Scheduler{Store: st, Queue: q, Logger: quiet}).Run(ctx)
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)

	c := &cluster{t: t, dsn: dsn, redisURL: redisURL, prefix: prefix, store: st, q: q, api: hs, ctx: ctx,
		procs: map[string]*exec.Cmd{}, exits: map[string]chan error{}}
	t.Cleanup(c.stopAll)
	return c
}

// startWorker launches a worker process with the PRD's production timers.
func (c *cluster) startWorker(id string) {
	c.t.Helper()
	cmd := exec.Command(workerBin)
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+c.dsn,
		"REDIS_URL="+c.redisURL,
		"RELAY_KEY_PREFIX="+c.prefix,
		"WORKER_ID="+id,
		"WORKER_CONCURRENCY=4",
		"METRICS_ADDR=127.0.0.1:0",
		"RECLAIM_INTERVAL=5s",
		"RECLAIM_IDLE=10s",
		"LEASE_INTERVAL=3s",
		"HEARTBEAT_INTERVAL=2s",
		"LOG_LEVEL=warn",
		"LOG_FORMAT=text",
	)
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	c.mu.Lock()
	c.procs[id], c.exits[id] = cmd, done
	c.mu.Unlock()
}

func (c *cluster) stopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, p := range c.procs {
		_ = p.Process.Kill()
		<-c.exits[id]
	}
	c.procs = map[string]*exec.Cmd{}
}

func (c *cluster) sigkill(id string) {
	c.mu.Lock()
	p, done := c.procs[id], c.exits[id]
	delete(c.procs, id)
	c.mu.Unlock()
	_ = p.Process.Kill()
	<-done
}

func (c *cluster) waitAlive(n int) {
	c.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ws, _ := c.q.Workers(c.ctx, 5*time.Second, 5*time.Second)
		alive := 0
		for _, w := range ws {
			if w.Alive {
				alive++
			}
		}
		if alive >= n {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("only %d of %d workers came up", alive, n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *cluster) post(path, body string) (int, map[string]any) {
	resp, err := http.Post(c.api.URL+path, "application/json", bytes.NewBufferString(body))
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (c *cluster) enqueue(n int, body string) []string {
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		code, out := c.post("/jobs", body)
		if code != http.StatusAccepted {
			c.t.Fatalf("enqueue: %d %v", code, out)
		}
		ids = append(ids, out["id"].(string))
	}
	return ids
}

// Acceptance: kill a worker mid-job; its jobs finish on another worker
// within 15 seconds.
func TestKillWorkerMidJobJobsFinishElsewhereWithin15s(t *testing.T) {
	c := newCluster(t)
	for _, id := range []string{"w1", "w2", "w3"} {
		c.startWorker(id)
	}
	c.waitAlive(3)

	ids := c.enqueue(24, `{"type":"sleep","payload":{"ms":2000}}`)
	time.Sleep(700 * time.Millisecond) // let every worker get mid-job

	held := map[string]bool{}
	for _, id := range ids {
		j, err := c.store.GetJob(c.ctx, id)
		if err == nil && j.Status == jobs.StatusRunning && j.WorkerID == "w1" {
			held[id] = true
		}
	}
	if len(held) == 0 {
		t.Fatal("w1 is not running any job; cannot test a mid-job kill")
	}

	// Kill through the API exactly like the dashboard button.
	if code, out := c.post("/workers/w1/kill", ""); code != http.StatusAccepted {
		t.Fatalf("kill: %d %v", code, out)
	}
	killedAt := time.Now()
	c.mu.Lock()
	exit := c.exits["w1"]
	delete(c.procs, "w1")
	c.mu.Unlock()
	select {
	case err := <-exit:
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != 1 {
			t.Fatalf("worker did not exit with status 1: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker ignored the kill")
	}

	var worst time.Duration
	for id := range held {
		for {
			j, _ := c.store.GetJob(c.ctx, id)
			if j.Status == jobs.StatusSucceeded {
				if j.WorkerID == "w1" {
					t.Fatalf("job %s finished on the dead worker", id)
				}
				worst = max(worst, time.Since(killedAt))
				break
			}
			if time.Since(killedAt) > 15*time.Second {
				t.Fatalf("job %s still %s %s after the kill", id, j.Status, time.Since(killedAt).Round(time.Millisecond))
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	t.Logf("%d jobs orphaned by the kill; last one finished %s after it", len(held), worst.Round(time.Millisecond))

	// Nothing else may be lost either.
	for _, id := range ids {
		deadline := time.Now().Add(20 * time.Second)
		for {
			j, _ := c.store.GetJob(c.ctx, id)
			if j.Status == jobs.StatusSucceeded {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("job %s never succeeded: %+v", id, j)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Acceptance: kill all workers, restart them, confirm zero jobs lost.
func TestKillAllWorkersRestartZeroLoss(t *testing.T) {
	c := newCluster(t)
	for _, id := range []string{"a1", "a2", "a3"} {
		c.startWorker(id)
	}
	c.waitAlive(3)

	const n = 300
	ids := c.enqueue(n, `{"type":"sleep","payload":{"ms":300}}`)
	time.Sleep(time.Second)
	for _, id := range []string{"a1", "a2", "a3"} {
		c.sigkill(id) // SIGKILL: no chance to clean up anything
	}
	counts, _ := c.store.CountByStatus(c.ctx)
	t.Logf("all workers SIGKILLed with %d running, %d queued", counts[jobs.StatusRunning], counts[jobs.StatusQueued])

	for _, id := range []string{"b1", "b2", "b3"} {
		c.startWorker(id)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		counts, _ = c.store.CountByStatus(c.ctx)
		if counts[jobs.StatusSucceeded] == n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not all jobs finished: %v", counts)
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, id := range ids {
		j, err := c.store.GetJob(c.ctx, id)
		if err != nil || j.Status != jobs.StatusSucceeded {
			t.Fatalf("job %s lost: %v %+v", id, err, j)
		}
	}
	d, _ := c.q.Depth(c.ctx)
	if d.Stream != 0 || d.Pending != 0 || d.Retry != 0 {
		t.Fatalf("queue not drained: %+v", d)
	}
}
