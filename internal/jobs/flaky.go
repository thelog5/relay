package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// FlakyPayload is {"fail_rate": 0.3, "ms": 50}. fail_rate is the probability
// that any single attempt fails; ms is an optional amount of work per attempt.
type FlakyPayload struct {
	FailRate float64 `json:"fail_rate"`
	MS       int     `json:"ms"`
}

// ErrFlaky is returned when a flaky job decides to fail.
var ErrFlaky = errors.New("flaky: simulated failure")

// FlakyHandler fails at a configurable rate (R2). It is the workhorse of the
// retry and dead-letter acceptance tests.
type FlakyHandler struct {
	// rand returns a float in [0,1). Overridable for tests.
	rand func() float64
}

func (FlakyHandler) parse(payload json.RawMessage) (FlakyPayload, error) {
	var p FlakyPayload
	if err := decode(payload, &p); err != nil {
		return p, err
	}
	if p.FailRate < 0 || p.FailRate > 1 {
		return p, errors.New("fail_rate must be between 0 and 1")
	}
	if p.MS < 0 || p.MS > MaxSleepMS {
		return p, errors.New("ms must be between 0 and 300000")
	}
	return p, nil
}

func (h FlakyHandler) Validate(payload json.RawMessage) error {
	_, err := h.parse(payload)
	return err
}

func (h FlakyHandler) Handle(ctx context.Context, job *Job) error {
	p, err := h.parse(job.Payload)
	if err != nil {
		return Permanent(err)
	}
	if err := sleepCtx(ctx, time.Duration(p.MS)*time.Millisecond); err != nil {
		return err
	}
	r := h.rand
	if r == nil {
		r = rand.Float64
	}
	if r() < p.FailRate {
		return fmt.Errorf("%w (attempt %d, fail_rate %.2f)", ErrFlaky, job.Attempts, p.FailRate)
	}
	return nil
}
