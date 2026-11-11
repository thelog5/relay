// Package api is Relay's HTTP server: job submission and lookup, stats, the
// SSE feed and dashboard, worker kill switch, and Prometheus metrics.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/thelog5/relay/internal/jobs"
	"github.com/thelog5/relay/internal/metrics"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/store"
	"github.com/thelog5/relay/web"
)

// Config tunes the API server.
type Config struct {
	// KillToken, if set, must be passed as ?token= to POST /workers/:id/kill.
	KillToken string
	// WorkerAlive is how recent a heartbeat must be for a worker to count
	// as alive. WorkerRecent is how long dead workers stay on the dashboard.
	WorkerAlive  time.Duration
	WorkerRecent time.Duration
	// MaxBodyBytes caps POST /jobs bodies.
	MaxBodyBytes int64
	// Limits guard a public demo; the zero value applies none.
	Limits Limits
	Logger *slog.Logger
}

// Server holds the API's dependencies.
type Server struct {
	cfg   Config
	store *store.Store
	q     *queue.Queue
	reg   jobs.Registry
	log   *slog.Logger
	hub   *Hub

	jobLimit  *rateLimiter
	killLimit *rateLimiter
	// unfinished is queued+running+retrying from the latest Snapshot, so
	// the MaxUnfinished check costs nothing per request.
	unfinished atomic.Int64
}

// New builds a Server. Call Handler for the http.Handler and RunHub to start
// the SSE fan-out.
func New(cfg Config, st *store.Store, q *queue.Queue, reg jobs.Registry) *Server {
	if cfg.WorkerAlive <= 0 {
		cfg.WorkerAlive = 6 * time.Second
	}
	if cfg.WorkerRecent <= 0 {
		cfg.WorkerRecent = 2 * time.Minute
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 1 << 20
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &Server{cfg: cfg, store: st, q: q, reg: reg, log: cfg.Logger,
		jobLimit:  newRateLimiter(cfg.Limits.JobsPerMinute, cfg.Limits.JobsBurst),
		killLimit: newRateLimiter(cfg.Limits.KillsPerMinute, cfg.Limits.KillsPerMinute)}
	s.hub = NewHub(s)
	return s
}

// Handler returns the routed, instrumented HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	route := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, instrument(pattern, h))
	}
	route("POST /jobs", s.handleCreateJob)
	route("GET /jobs", s.handleListJobs)
	route("GET /jobs/{id}", s.handleGetJob)
	route("POST /jobs/{id}/retry", s.handleRetryJob)
	route("DELETE /jobs/{id}", s.handleDeleteJob)
	route("GET /stats", s.handleStats)
	route("POST /workers/{id}/kill", s.handleKillWorker)
	route("GET /healthz", s.handleHealth)
	route("GET /{$}", s.handleDashboard)
	// SSE and metrics are not wrapped: SSE is long-lived and would skew the
	// latency histogram, and /metrics should not measure itself.
	mux.HandleFunc("GET /events", s.handleEvents)
	mux.Handle("GET /metrics", metrics.Handler())
	mux.Handle("GET /static/", http.StripPrefix("/static/", cacheFor(24*time.Hour, http.FileServerFS(web.Static))))
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=86400")
		http.ServeFileFS(w, r, web.Static, "favicon-32.png")
	})
	return mux
}

// cacheFor lets browsers keep static assets for d.
func cacheFor(d time.Duration, h http.Handler) http.Handler {
	v := "public, max-age=" + strconv.Itoa(int(d.Seconds()))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", v)
		h.ServeHTTP(w, r)
	})
}

// statusRecorder captures the response code for metrics.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func instrument(route string, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		h(rec, r)
		metrics.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
		metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(rec.code)).Inc()
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

type apiError struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, apiError{Error: msg})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "postgres: "+err.Error())
		return
	}
	if err := s.q.Client().Ping(ctx).Err(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "redis: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// storeError maps store errors to HTTP responses.
func storeError(w http.ResponseWriter, err error) {
	var conflict *store.ConflictError
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.As(err, &conflict):
		writeError(w, http.StatusConflict, conflict.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}
