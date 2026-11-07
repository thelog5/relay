package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/thelog5/relay/internal/jobs"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/store"
	"github.com/thelog5/relay/internal/testutil"
)

type env struct {
	t     *testing.T
	srv   *httptest.Server
	api   *Server
	store *store.Store
	q     *queue.Queue
}

func newEnv(t *testing.T, cfg Config) *env {
	t.Helper()
	dsn := testutil.PostgresDSN(t)
	redisURL, prefix := testutil.RedisURL(t)
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
	a := New(cfg, st, q, jobs.DefaultRegistry())
	a.hub.batchEvery = 20 * time.Millisecond
	a.hub.statsEvery = 50 * time.Millisecond
	go a.RunHub(ctx)
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	return &env{t: t, srv: srv, api: a, store: st, q: q}
}

func (e *env) do(method, path, body string) (int, map[string]any) {
	e.t.Helper()
	req, _ := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return resp.StatusCode, out
}

func TestCreateJobReturns202AndEnqueues(t *testing.T) {
	e := newEnv(t, Config{})
	code, body := e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":5}}`)
	if code != http.StatusAccepted {
		t.Fatalf("code %d body %v", code, body)
	}
	id, _ := body["id"].(string)
	if id == "" || body["status"] != "queued" {
		t.Fatalf("body %v", body)
	}
	j, err := e.store.GetJob(context.Background(), id)
	if err != nil || j.MaxAttempts != jobs.DefaultMaxAttempts {
		t.Fatalf("stored job: %v %+v", err, j)
	}
	d, _ := e.q.Depth(context.Background())
	if d.Backlog != 1 {
		t.Fatalf("job not on stream: %+v", d)
	}
}

func TestCreateJobValidation(t *testing.T) {
	e := newEnv(t, Config{})
	cases := map[string]string{
		"unknown type":      `{"type":"teleport"}`,
		"bad payload":       `{"type":"sleep","payload":{"ms":-1}}`,
		"unknown field":     `{"type":"sleep","payload":{"ms":1},"priority":9}`,
		"max_attempts zero": `{"type":"sleep","payload":{"ms":1},"max_attempts":0}`,
		"not json":          `sleep please`,
		"webhook no url":    `{"type":"webhook","payload":{}}`,
	}
	for name, body := range cases {
		if code, out := e.do("POST", "/jobs", body); code != http.StatusBadRequest {
			t.Errorf("%s: code %d %v", name, code, out)
		}
	}
	code, out := e.do("POST", "/jobs", `{"type":"flaky","payload":{"fail_rate":0.3},"max_attempts":2}`)
	if code != http.StatusAccepted {
		t.Fatalf("custom max_attempts: %d %v", code, out)
	}
	j, _ := e.store.GetJob(context.Background(), out["id"].(string))
	if j.MaxAttempts != 2 {
		t.Fatalf("max_attempts not stored: %+v", j)
	}
}

func TestGetJobIncludesHistory(t *testing.T) {
	e := newEnv(t, Config{})
	_, created := e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":1}}`)
	id := created["id"].(string)
	code, body := e.do("GET", "/jobs/"+id, "")
	if code != 200 || body["id"] != id || body["status"] != "queued" {
		t.Fatalf("%d %v", code, body)
	}
	evs := body["events"].([]any)
	if len(evs) != 1 || evs[0].(map[string]any)["to_status"] != "queued" {
		t.Fatalf("events: %v", evs)
	}
	if code, _ := e.do("GET", "/jobs/00000000-0000-0000-0000-000000000000", ""); code != 404 {
		t.Fatalf("missing job code %d", code)
	}
	if code, _ := e.do("GET", "/jobs/not-a-uuid", ""); code != 400 {
		t.Fatalf("bad id code %d", code)
	}
}

func makeDead(t *testing.T, e *env) string {
	t.Helper()
	_, created := e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":1}}`)
	id := created["id"].(string)
	ctx := context.Background()
	msgs, _ := e.q.Read(ctx, "w", 1, 50*time.Millisecond)
	e.store.Transition(ctx, id, store.TransitionOpts{From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusRunning, IncAttempts: true, WorkerID: "w"})
	e.store.Transition(ctx, id, store.TransitionOpts{From: []jobs.Status{jobs.StatusRunning}, To: jobs.StatusDead, Error: "nope"})
	e.q.DeadLetter(ctx, msgs[0], "nope")
	return id
}

func TestListByStatusRetryAndDeleteDead(t *testing.T) {
	e := newEnv(t, Config{})
	dead := makeDead(t, e)
	e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":1}}`)

	code, body := e.do("GET", "/jobs?status=dead", "")
	list := body["jobs"].([]any)
	if code != 200 || len(list) != 1 || list[0].(map[string]any)["id"] != dead {
		t.Fatalf("%d %v", code, body)
	}
	if code, _ := e.do("GET", "/jobs?status=exploded", ""); code != 400 {
		t.Fatalf("bad status code %d", code)
	}

	if code, out := e.do("POST", "/jobs/"+dead+"/retry", ""); code != http.StatusAccepted {
		t.Fatalf("retry: %d %v", code, out)
	}
	j, _ := e.store.GetJob(context.Background(), dead)
	if j.Status != jobs.StatusQueued || j.Attempts != 0 {
		t.Fatalf("after retry: %+v", j)
	}
	d, _ := e.q.Depth(context.Background())
	if d.Dead != 0 || d.Backlog != 2 {
		t.Fatalf("depth after retry: %+v", d)
	}
	if code, _ := e.do("POST", "/jobs/"+dead+"/retry", ""); code != http.StatusConflict {
		t.Fatalf("retrying a queued job should conflict, got %d", code)
	}

	dead2 := makeDead(t, e)
	if code, _ := e.do("DELETE", "/jobs/"+dead2, ""); code != http.StatusNoContent {
		t.Fatalf("delete code %d", code)
	}
	if code, _ := e.do("GET", "/jobs/"+dead2, ""); code != 404 {
		t.Fatal("deleted job still there")
	}
}

func TestStats(t *testing.T) {
	e := newEnv(t, Config{})
	e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":1}}`)
	makeDead(t, e)
	e.q.Heartbeat(context.Background(), queue.WorkerInfo{ID: "w-1", Concurrency: 4, StartedAt: time.Now()})
	code, body := e.do("GET", "/stats", "")
	if code != 200 {
		t.Fatalf("code %d", code)
	}
	st := body["statuses"].(map[string]any)
	if st["queued"].(float64) != 1 || st["dead"].(float64) != 1 || body["total"].(float64) != 2 {
		t.Fatalf("statuses %v", body)
	}
	ws := body["workers"].([]any)
	if len(ws) != 1 || ws[0].(map[string]any)["id"] != "w-1" {
		t.Fatalf("workers %v", ws)
	}
}

func TestKillRequiresTokenWhenConfigured(t *testing.T) {
	e := newEnv(t, Config{KillToken: "s3cret"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := e.q.SubscribeControl(ctx, "victim")
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := e.do("POST", "/workers/victim/kill", ""); code != http.StatusForbidden {
		t.Fatalf("no token: %d", code)
	}
	if code, _ := e.do("POST", "/workers/victim/kill?token=wrong", ""); code != http.StatusForbidden {
		t.Fatalf("wrong token: %d", code)
	}
	if code, _ := e.do("POST", "/workers/victim/kill?token=s3cret", ""); code != http.StatusAccepted {
		t.Fatalf("right token: %d", code)
	}
	select {
	case m := <-ch:
		if m.WorkerID != "victim" {
			t.Fatalf("%+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("kill not delivered")
	}
}

func TestKillUnknownWorkerIs404(t *testing.T) {
	e := newEnv(t, Config{})
	if code, _ := e.do("POST", "/workers/ghost/kill", ""); code != http.StatusNotFound {
		t.Fatalf("code %d", code)
	}
}

func TestEventsStreamPrimesAndStreams(t *testing.T) {
	e := newEnv(t, Config{})
	e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":1}}`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", e.srv.URL+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type %q", ct)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	seen := map[string]bool{}
	posted := false
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "event: ") {
			seen[strings.TrimPrefix(line, "event: ")] = true
		}
		if strings.HasPrefix(line, "data: ") && strings.Contains(line, `"to":"queued"`) && posted {
			return // a live event arrived through Redis pub/sub and the hub
		}
		if seen["stats"] && seen["jobs"] && !posted {
			posted = true
			e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":1}}`)
		}
	}
	t.Fatalf("stream ended early; saw %v", seen)
}

func TestDashboardAndHealth(t *testing.T) {
	e := newEnv(t, Config{})
	resp, err := http.Get(e.srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(b), "<title>Relay") {
		t.Fatalf("dashboard: %d", resp.StatusCode)
	}
	if code, body := e.do("GET", "/healthz", ""); code != 200 || body["status"] != "ok" {
		t.Fatalf("health %d %v", code, body)
	}
	resp, _ = http.Get(e.srv.URL + "/metrics")
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), "relay_jobs_by_status") {
		t.Fatal("metrics missing relay gauges")
	}
}

// Regression: a k6 run with request timeouts left jobs in queued with nothing
// on the stream, because the client's cancellation aborted the XADD after
// the Postgres insert. Enqueue must finish once it has started.
func TestEnqueueSurvivesClientCancellation(t *testing.T) {
	e := newEnv(t, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest("POST", "/jobs", nil).WithContext(ctx)
	j, err := e.api.Enqueue(req, CreateJobRequest{Type: "sleep", Payload: json.RawMessage(`{"ms":1}`)})
	if err != nil {
		t.Fatalf("enqueue with cancelled client context: %v", err)
	}
	d, _ := e.q.Depth(context.Background())
	if d.Backlog != 1 {
		t.Fatalf("job %s not on the stream: %+v", j.ID, d)
	}
}
