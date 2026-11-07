package jobs

import (
	"math/rand/v2"
	"time"
)

// Backoff computes retry delays: exponential from Base, doubled per attempt,
// capped at Max, with up to JitterFrac extra random delay so a burst of
// failures does not retry in lockstep (R4).
//
// With the defaults the un-jittered delays are 1s, 2s, 4s, 8s ... 60s.
type Backoff struct {
	Base       time.Duration
	Max        time.Duration
	JitterFrac float64
	// rand returns a float in [0,1). Overridable for tests.
	rand func() float64
}

// DefaultBackoff is the policy from the PRD: 1s, 2s, 4s ... capped at 60s,
// plus up to 20% jitter.
func DefaultBackoff() Backoff {
	return Backoff{Base: time.Second, Max: 60 * time.Second, JitterFrac: 0.2}
}

// Delay returns how long to wait before retry number `attempt`, where attempt
// is the number of attempts already made (1 after the first failure).
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.Base
	for i := 1; i < attempt && d < b.Max; i++ {
		d *= 2
	}
	if d > b.Max {
		d = b.Max
	}
	if b.JitterFrac > 0 {
		r := b.rand
		if r == nil {
			r = rand.Float64
		}
		d += time.Duration(float64(d) * b.JitterFrac * r())
	}
	if d > b.Max {
		d = b.Max
	}
	return d
}
