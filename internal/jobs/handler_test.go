package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegistryLookup(t *testing.T) {
	r := DefaultRegistry()
	for _, typ := range []string{"sleep", "flaky", "webhook"} {
		if _, err := r.Lookup(typ); err != nil {
			t.Fatalf("lookup %s: %v", typ, err)
		}
	}
	if _, err := r.Lookup("teleport"); err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestPermanent(t *testing.T) {
	base := errors.New("boom")
	if IsPermanent(base) {
		t.Fatal("plain error should not be permanent")
	}
	if !IsPermanent(Permanent(base)) {
		t.Fatal("wrapped error should be permanent")
	}
	if !IsPermanent(fmt.Errorf("context: %w", Permanent(base))) {
		t.Fatal("permanence should survive further wrapping")
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) should be nil")
	}
}

func TestSleepHandler(t *testing.T) {
	h := SleepHandler{}
	if err := h.Validate(json.RawMessage(`{"ms":10}`)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{`{"ms":-1}`, `{"ms":999999999}`, `{"millis":5}`, `[]`} {
		if err := h.Validate(json.RawMessage(bad)); err == nil {
			t.Errorf("expected %s to be rejected", bad)
		}
	}
	start := time.Now()
	if err := h.Handle(context.Background(), &Job{Payload: json.RawMessage(`{"ms":30}`)}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("sleep returned early")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.Handle(ctx, &Job{Payload: json.RawMessage(`{"ms":10000}`)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if err := h.Handle(context.Background(), &Job{Payload: json.RawMessage(`{"ms":"x"}`)}); !IsPermanent(err) {
		t.Fatalf("bad payload should be permanent, got %v", err)
	}
}

func TestFlakyHandler(t *testing.T) {
	always := FlakyHandler{rand: func() float64 { return 0 }}
	never := FlakyHandler{rand: func() float64 { return 0.99 }}
	job := &Job{Payload: json.RawMessage(`{"fail_rate":0.3}`)}
	if err := always.Handle(context.Background(), job); !errors.Is(err, ErrFlaky) {
		t.Fatalf("expected ErrFlaky, got %v", err)
	}
	if err := never.Handle(context.Background(), job); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if err := (FlakyHandler{}).Validate(json.RawMessage(`{"fail_rate":1.5}`)); err == nil {
		t.Fatal("fail_rate > 1 should be rejected")
	}

	// Real randomness: 30% configured should land near 30% over many runs.
	random := FlakyHandler{}
	fails := 0
	const n = 5000
	for i := 0; i < n; i++ {
		if random.Handle(context.Background(), job) != nil {
			fails++
		}
	}
	if rate := float64(fails) / n; rate < 0.25 || rate > 0.35 {
		t.Fatalf("observed fail rate %.3f, want about 0.30", rate)
	}
}

func TestWebhookHandler(t *testing.T) {
	var hits atomic.Int32
	var gotKey, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		gotKey = r.Header.Get("Idempotency-Key")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(http.StatusNoContent)
		case "/500":
			w.WriteHeader(http.StatusInternalServerError)
		case "/429":
			w.WriteHeader(http.StatusTooManyRequests)
		case "/400":
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	h := NewWebhookHandler()
	mk := func(path string) *Job {
		return &Job{ID: "job-123", Attempts: 2, Payload: json.RawMessage(`{"url":"` + srv.URL + path + `","body":{"hello":"world"}}`)}
	}

	if err := h.Handle(context.Background(), mk("/ok")); err != nil {
		t.Fatalf("ok: %v", err)
	}
	if gotKey != "job-123" {
		t.Fatalf("idempotency key = %q", gotKey)
	}
	if gotBody != `{"hello":"world"}` {
		t.Fatalf("body = %q", gotBody)
	}

	if err := h.Handle(context.Background(), mk("/500")); err == nil || IsPermanent(err) {
		t.Fatalf("500 should be retryable, got %v", err)
	}
	if err := h.Handle(context.Background(), mk("/429")); err == nil || IsPermanent(err) {
		t.Fatalf("429 should be retryable, got %v", err)
	}
	if err := h.Handle(context.Background(), mk("/400")); !IsPermanent(err) {
		t.Fatalf("400 should be permanent, got %v", err)
	}
	if hits.Load() != 4 {
		t.Fatalf("expected 4 hits, got %d", hits.Load())
	}
}

func TestWebhookValidation(t *testing.T) {
	h := NewWebhookHandler()
	for _, bad := range []string{`{}`, `{"url":"ftp://x"}`, `{"url":"/relative"}`, `{"url":"http://x","timeout_ms":99999}`} {
		if err := h.Validate(json.RawMessage(bad)); err == nil {
			t.Errorf("expected %s to be rejected", bad)
		}
	}
}

func TestWebhookBlocksMetadataAddress(t *testing.T) {
	h := NewWebhookHandler()
	job := &Job{ID: "j", Payload: json.RawMessage(`{"url":"http://169.254.169.254/latest/meta-data","timeout_ms":1000}`)}
	err := h.Handle(context.Background(), job)
	if err == nil {
		t.Fatal("expected link-local address to be blocked")
	}
}
