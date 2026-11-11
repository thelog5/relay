package api

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRateLimiterBucket(t *testing.T) {
	now := time.Unix(0, 0)
	l := newRateLimiter(60, 3) // one per second, burst of three
	l.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		if ok, _ := l.allow("a"); !ok {
			t.Fatalf("request %d inside the burst was refused", i+1)
		}
	}
	ok, wait := l.allow("a")
	if ok || wait <= 0 || wait > time.Second {
		t.Fatalf("fourth request: ok=%v wait=%v", ok, wait)
	}
	if ok, _ := l.allow("b"); !ok {
		t.Fatal("another address shares the first one's bucket")
	}
	now = now.Add(time.Second)
	if ok, _ := l.allow("a"); !ok {
		t.Fatal("bucket did not refill after a second")
	}
	if newRateLimiter(0, 10) != nil {
		t.Fatal("zero rate must mean no limiter")
	}
	var off *rateLimiter
	if ok, _ := off.allow("x"); !ok {
		t.Fatal("nil limiter must allow everything")
	}
}

func TestClientIP(t *testing.T) {
	r := httptest.NewRequest("POST", "/jobs", nil)
	r.RemoteAddr = "10.0.0.9:5555"
	r.Header.Set("X-Forwarded-For", "6.6.6.6, 203.0.113.7")
	if got := clientIP(r, 0); got != "10.0.0.9" {
		t.Fatalf("hops=0: %s", got)
	}
	// One trusted proxy: the entry it appended, not the spoofable first one.
	if got := clientIP(r, 1); got != "203.0.113.7" {
		t.Fatalf("hops=1: %s", got)
	}
	if got := clientIP(r, 5); got != "10.0.0.9" {
		t.Fatalf("more hops than entries should fall back to the peer: %s", got)
	}
}

func TestDemoLimitsOnCreateJob(t *testing.T) {
	e := newEnv(t, Config{Limits: Limits{
		JobsPerMinute: 60, JobsBurst: 3,
		DisabledTypes: []string{"webhook"},
		MaxSleepMS:    10000,
	}})
	if code, body := e.do("POST", "/jobs", `{"type":"webhook","payload":{"url":"http://example.com"}}`); code != 400 {
		t.Fatalf("disabled type: %d %v", code, body)
	}
	if code, body := e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":60000}}`); code != 400 {
		t.Fatalf("sleep over the cap: %d %v", code, body)
	}
	// Refused requests still spend tokens, so the third one is the last of
	// the burst of three.
	if code, body := e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":5}}`); code != 202 {
		t.Fatalf("third request inside the burst: %d %v", code, body)
	}
	if code, body := e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":5}}`); code != 429 {
		t.Fatalf("fourth request should be rate limited (burst 3): %d %v", code, body)
	}
}

func TestQueueFullRejectsNewJobs(t *testing.T) {
	e := newEnv(t, Config{Limits: Limits{MaxUnfinished: 2}})
	for i := 0; i < 2; i++ {
		if code, _ := e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":5}}`); code != 202 {
			t.Fatalf("job %d: %d", i, code)
		}
	}
	if _, err := e.api.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if code, body := e.do("POST", "/jobs", `{"type":"sleep","payload":{"ms":5}}`); code != 503 {
		t.Fatalf("queue full: %d %v", code, body)
	}
}

func TestKillIsRateLimited(t *testing.T) {
	e := newEnv(t, Config{Limits: Limits{KillsPerMinute: 1}})
	e.do("POST", "/workers/nobody/kill", "")
	if code, _ := e.do("POST", "/workers/nobody/kill", ""); code != 429 {
		t.Fatalf("second kill in a minute: %d", code)
	}
}
