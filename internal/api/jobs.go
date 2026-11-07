package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/thelog5/relay/internal/jobs"
	"github.com/thelog5/relay/internal/metrics"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/store"
)

// CreateJobRequest is the body of POST /jobs (R1).
type CreateJobRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts *int            `json:"max_attempts,omitempty"`
}

// CreateJobResponse is returned with 202 Accepted.
type CreateJobResponse struct {
	ID     string      `json:"id"`
	Status jobs.Status `json:"status"`
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	var req CreateJobRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	j, err := s.Enqueue(r, req)
	if err != nil {
		var ve validationError
		if errors.As(err, &ve) {
			writeError(w, http.StatusBadRequest, ve.Error())
			return
		}
		s.log.Error("enqueue failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "could not enqueue job: "+err.Error())
		return
	}
	w.Header().Set("Location", "/jobs/"+j.ID)
	writeJSON(w, http.StatusAccepted, CreateJobResponse{ID: j.ID, Status: j.Status})
}

type validationError struct{ msg string }

func (v validationError) Error() string { return v.msg }

// Enqueue validates and stores a job, then puts it on the stream.
//
// Postgres first, then Redis: if the XADD fails the row is marked dead with
// the reason, so a job is never silently stuck in queued.
func (s *Server) Enqueue(r *http.Request, req CreateJobRequest) (*jobs.Job, error) {
	// Once the row is written, the XADD (or the dead-marking fallback) must
	// happen even if the client hangs up. A load test caught this: request
	// timeouts cancelled the context between the two writes and left jobs
	// stuck in queued with nothing on the stream.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Second)
	defer cancel()
	h, err := s.reg.Lookup(req.Type)
	if err != nil {
		return nil, validationError{err.Error()}
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		req.Payload = json.RawMessage("{}")
	}
	if err := h.Validate(req.Payload); err != nil {
		return nil, validationError{fmt.Sprintf("%s: %v", req.Type, err)}
	}
	maxAttempts := jobs.DefaultMaxAttempts
	if req.MaxAttempts != nil {
		maxAttempts = *req.MaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > 100 {
		return nil, validationError{"max_attempts must be between 1 and 100"}
	}

	j, err := s.store.CreateJob(ctx, uuid.NewString(), req.Type, req.Payload, maxAttempts)
	if err != nil {
		return nil, err
	}
	if _, err := s.q.Enqueue(ctx, j.ID); err != nil {
		_, _, _ = s.store.Transition(ctx, j.ID, store.TransitionOpts{
			From: []jobs.Status{jobs.StatusQueued}, To: jobs.StatusDead, WorkerID: "api", Error: "enqueue failed: " + err.Error(),
		})
		return nil, err
	}
	metrics.JobsEnqueued.WithLabelValues(j.Type).Inc()
	_ = s.q.PublishEvent(ctx, queue.StatusEvent{JobID: j.ID, Type: j.Type, To: string(jobs.StatusQueued), At: j.CreatedAt})
	return j, nil
}

// JobDetail is GET /jobs/:id: the job plus its full status history.
type JobDetail struct {
	*jobs.Job
	Events []jobs.Event `json:"events"`
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "job id must be a UUID")
		return
	}
	j, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		storeError(w, err)
		return
	}
	evs, err := s.store.JobEvents(r.Context(), id)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, JobDetail{Job: j, Events: evs})
}

// JobList is GET /jobs?status=dead&limit=50.
type JobList struct {
	Jobs []jobs.Job `json:"jobs"`
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	f := store.ListFilter{Limit: 50}
	if st := r.URL.Query().Get("status"); st != "" {
		parsed, err := jobs.ParseStatus(st)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		f.Status = parsed
	}
	if l := r.URL.Query().Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "limit must be 1-500")
			return
		}
		f.Limit = n
	}
	list, err := s.store.ListJobs(r.Context(), f)
	if err != nil {
		storeError(w, err)
		return
	}
	if list == nil {
		list = []jobs.Job{}
	}
	writeJSON(w, http.StatusOK, JobList{Jobs: list})
}

// handleRetryJob re-queues a dead job with a fresh attempt budget (R12).
func (s *Server) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "job id must be a UUID")
		return
	}
	j, _, err := s.store.Transition(ctx, id, store.TransitionOpts{
		From: []jobs.Status{jobs.StatusDead}, To: jobs.StatusQueued, WorkerID: "dashboard", ResetAttempts: true,
	})
	if err != nil {
		storeError(w, err)
		return
	}
	if _, err := s.q.Enqueue(ctx, id); err != nil {
		writeError(w, http.StatusServiceUnavailable, "job marked queued but enqueue failed: "+err.Error())
		return
	}
	if _, err := s.q.RemoveDead(ctx, id); err != nil {
		s.log.Warn("could not remove job from dead-letter stream", "job_id", id, "err", err)
	}
	_ = s.q.PublishEvent(ctx, queue.StatusEvent{JobID: id, Type: j.Type, From: string(jobs.StatusDead), To: string(jobs.StatusQueued), WorkerID: "dashboard", At: time.Now()})
	writeJSON(w, http.StatusAccepted, CreateJobResponse{ID: id, Status: j.Status})
}

// handleDeleteJob removes a dead (or succeeded) job (R12).
func (s *Server) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "job id must be a UUID")
		return
	}
	if err := s.store.DeleteJob(r.Context(), id); err != nil {
		storeError(w, err)
		return
	}
	if _, err := s.q.RemoveDead(r.Context(), id); err != nil {
		s.log.Warn("could not remove job from dead-letter stream", "job_id", id, "err", err)
	}
	w.WriteHeader(http.StatusNoContent)
}
