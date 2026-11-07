package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/thelog5/relay/internal/queue"
)

// Hub fans one Redis subscription out to every open dashboard. Job events are
// batched every 200ms, and a stats snapshot goes out every second, so ten
// thousand status changes a second do not become ten thousand SSE writes per
// browser.
type Hub struct {
	srv *Server

	mu      sync.Mutex
	clients map[chan []byte]struct{}

	batchEvery time.Duration
	statsEvery time.Duration
	maxBatch   int
}

// NewHub builds a hub for srv.
func NewHub(srv *Server) *Hub {
	return &Hub{
		srv:        srv,
		clients:    make(map[chan []byte]struct{}),
		batchEvery: 200 * time.Millisecond,
		statsEvery: time.Second,
		maxBatch:   100,
	}
}

// RunHub starts the fan-out and the stats loop. It returns when ctx ends.
func (s *Server) RunHub(ctx context.Context) { s.hub.Run(ctx) }

func (h *Hub) subscribe() chan []byte {
	ch := make(chan []byte, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
}

func (h *Hub) clientCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

func (h *Hub) broadcast(event string, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	msg := []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", event, data))
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default: // this browser is behind; skip rather than block everyone
		}
	}
}

// JobBatch is one "jobs" SSE message.
type JobBatch struct {
	Events  []queue.StatusEvent `json:"events"`
	Dropped int                 `json:"dropped"` // events beyond maxBatch in this window
}

// Run consumes Redis events and ticks stats until ctx is cancelled.
func (h *Hub) Run(ctx context.Context) {
	events := h.srv.q.SubscribeEvents(ctx)
	batchT := time.NewTicker(h.batchEvery)
	statsT := time.NewTicker(h.statsEvery)
	defer batchT.Stop()
	defer statsT.Stop()

	var batch []queue.StatusEvent
	dropped := 0
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			if len(batch) < h.maxBatch {
				batch = append(batch, ev)
			} else {
				dropped++
			}
		case <-batchT.C:
			if len(batch) > 0 || dropped > 0 {
				h.broadcast("jobs", JobBatch{Events: batch, Dropped: dropped})
				batch, dropped = nil, 0
			}
		case <-statsT.C:
			sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			st, err := h.srv.Snapshot(sctx)
			cancel()
			if err != nil {
				h.srv.log.Warn("stats snapshot failed", "err", err)
				continue
			}
			if h.clientCount() > 0 {
				h.broadcast("stats", st)
			}
		}
	}
}

// handleEvents is GET /events: a Server-Sent Events stream of "stats" and
// "jobs" messages for the dashboard (R8).
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := s.hub.subscribe()
	defer s.hub.unsubscribe(ch)

	// Prime a new dashboard with current stats and the recent feed.
	if st, err := s.Snapshot(r.Context()); err == nil {
		b, _ := json.Marshal(st)
		fmt.Fprintf(w, "event: stats\ndata: %s\n\n", b)
	}
	if feed, err := s.store.RecentEvents(r.Context(), 50); err == nil {
		evs := make([]queue.StatusEvent, 0, len(feed))
		for i := len(feed) - 1; i >= 0; i-- {
			f := feed[i]
			evs = append(evs, queue.StatusEvent{
				JobID: f.JobID, Type: f.Type, From: string(f.FromStatus), To: string(f.ToStatus),
				WorkerID: f.WorkerID, Error: f.Error, At: f.At,
			})
		}
		b, _ := json.Marshal(JobBatch{Events: evs})
		fmt.Fprintf(w, "event: jobs\ndata: %s\n\n", b)
	}
	flusher.Flush()

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			if _, err := w.Write(msg); err != nil {
				return
			}
			flusher.Flush()
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
