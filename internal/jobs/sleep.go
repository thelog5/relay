package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// MaxSleepMS bounds sleep jobs so nobody parks a worker for an hour.
const MaxSleepMS = 5 * 60 * 1000

// SleepPayload is {"ms": 250}.
type SleepPayload struct {
	MS int `json:"ms"`
}

// SleepHandler waits N milliseconds and succeeds. Used for throughput tests
// (ms=0) and for crash demos (ms of a few seconds keeps a job mid-flight).
type SleepHandler struct{}

func (SleepHandler) parse(payload json.RawMessage) (SleepPayload, error) {
	var p SleepPayload
	if err := decode(payload, &p); err != nil {
		return p, err
	}
	if p.MS < 0 || p.MS > MaxSleepMS {
		return p, errors.New("ms must be between 0 and 300000")
	}
	return p, nil
}

func (h SleepHandler) Validate(payload json.RawMessage) error {
	_, err := h.parse(payload)
	return err
}

func (h SleepHandler) Handle(ctx context.Context, job *Job) error {
	p, err := h.parse(job.Payload)
	if err != nil {
		return Permanent(err)
	}
	return sleepCtx(ctx, time.Duration(p.MS)*time.Millisecond)
}
