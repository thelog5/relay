package api

import (
	"context"
	"net/http"
	"time"

	"github.com/thelog5/relay/internal/jobs"
	"github.com/thelog5/relay/internal/metrics"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/store"
)

// Stats is GET /stats: counts per status and per worker (R8).
type Stats struct {
	Statuses map[jobs.Status]int64 `json:"statuses"`
	Total    int64                 `json:"total"`
	Queue    queue.Depth           `json:"queue"`
	Workers  []queue.WorkerInfo    `json:"workers"`
	Recovery store.RecoveryStats   `json:"recovery"`
	At       time.Time             `json:"at"`
}

// Snapshot gathers stats and refreshes the Prometheus gauges as a side
// effect, so /metrics stays current even with no dashboard open.
func (s *Server) Snapshot(ctx context.Context) (*Stats, error) {
	counts, err := s.store.CountByStatus(ctx)
	if err != nil {
		return nil, err
	}
	depth, err := s.q.Depth(ctx)
	if err != nil {
		return nil, err
	}
	workers, err := s.q.Workers(ctx, s.cfg.WorkerAlive, s.cfg.WorkerRecent)
	if err != nil {
		return nil, err
	}
	rec, err := s.store.Recovery(ctx)
	if err != nil {
		return nil, err
	}
	st := &Stats{Statuses: counts, Queue: depth, Workers: workers, Recovery: rec, At: time.Now()}
	alive := 0
	for _, w := range workers {
		if w.Alive {
			alive++
		}
	}
	unfinished := counts[jobs.StatusQueued] + counts[jobs.StatusRunning] + counts[jobs.StatusRetryScheduled]
	s.unfinished.Store(unfinished)
	if unfinished > 0 {
		s.touch() // not idle while there is work in flight (idle.go)
	}
	for status, n := range counts {
		st.Total += n
		metrics.JobsByStatus.WithLabelValues(string(status)).Set(float64(n))
	}
	metrics.QueueDepth.WithLabelValues("backlog").Set(float64(depth.Backlog))
	metrics.QueueDepth.WithLabelValues("pending").Set(float64(depth.Pending))
	metrics.QueueDepth.WithLabelValues("retry").Set(float64(depth.Retry))
	metrics.QueueDepth.WithLabelValues("dead").Set(float64(depth.Dead))
	metrics.WorkersAlive.Set(float64(alive))
	return st, nil
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.Snapshot(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// handleKillWorker makes one worker exit mid-job with os.Exit(1). Demo only.
func (s *Server) handleKillWorker(w http.ResponseWriter, r *http.Request) {
	if s.cfg.KillToken != "" && r.URL.Query().Get("token") != s.cfg.KillToken {
		writeError(w, http.StatusForbidden, "kill requires ?token=")
		return
	}
	if limited(w, s.killLimit, clientIP(r, s.cfg.Limits.ProxyHops), "kills") {
		return
	}
	id := r.PathValue("id")
	n, err := s.q.PublishKill(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if n == 0 {
		writeError(w, http.StatusNotFound, "no live worker is listening with id "+id)
		return
	}
	metrics.WorkerKills.Inc()
	s.log.Warn("kill sent to worker", "worker", id)
	writeJSON(w, http.StatusAccepted, map[string]string{"killed": id})
}
