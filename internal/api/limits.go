package api

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Limits protect a public demo from being flooded. Every zero value turns
// that limit off, which is the default for local and private deployments.
type Limits struct {
	// JobsPerMinute and JobsBurst rate-limit POST /jobs per client address
	// with a token bucket. The dashboard's largest burst button sends 500.
	JobsPerMinute int
	JobsBurst     int
	// KillsPerMinute rate-limits the worker kill switch per client address.
	KillsPerMinute int
	// MaxUnfinished rejects new jobs with 503 while at least this many are
	// queued, running or waiting to retry, so the database cannot be filled.
	MaxUnfinished int64
	// DisabledTypes are job types this deployment refuses (the public demo
	// turns off webhook jobs so visitors cannot make it send requests).
	DisabledTypes []string
	// MaxSleepMS caps sleep jobs so nobody parks every worker for minutes.
	MaxSleepMS int
	// ProxyHops is the number of trusted reverse proxies in front of the
	// api. With 1, the client address is the last X-Forwarded-For entry
	// (the one the platform's proxy appended); 0 uses the TCP peer.
	ProxyHops int
}

// Active reports whether any limit is switched on.
func (l Limits) Active() bool {
	return l.JobsPerMinute > 0 || l.KillsPerMinute > 0 || l.MaxUnfinished > 0 || len(l.DisabledTypes) > 0 || l.MaxSleepMS > 0
}

func (l Limits) disabled(jobType string) bool {
	for _, t := range l.DisabledTypes {
		if strings.EqualFold(strings.TrimSpace(t), jobType) {
			return true
		}
	}
	return false
}

// checkJob applies the per-job demo limits after the body is decoded.
func (l Limits) checkJob(req CreateJobRequest) error {
	if l.disabled(req.Type) {
		return validationError{req.Type + " jobs are turned off on this deployment"}
	}
	if l.MaxSleepMS > 0 && req.Type == "sleep" && len(req.Payload) > 0 {
		var p struct {
			MS int `json:"ms"`
		}
		if json.Unmarshal(req.Payload, &p) == nil && p.MS > l.MaxSleepMS {
			return validationError{fmt.Sprintf("sleep: ms is capped at %d on this deployment", l.MaxSleepMS)}
		}
	}
	return nil
}

// clientIP picks the address to rate-limit by.
func clientIP(r *http.Request, hops int) string {
	if hops > 0 {
		var parts []string
		for _, v := range r.Header.Values("X-Forwarded-For") {
			for _, p := range strings.Split(v, ",") {
				if p = strings.TrimSpace(p); p != "" {
					parts = append(parts, p)
				}
			}
		}
		if i := len(parts) - hops; i >= 0 && i < len(parts) {
			return parts[i]
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// rateLimiter is a token bucket per key. Idle buckets are swept so the map
// stays small however many addresses visit.
type rateLimiter struct {
	mu      sync.Mutex
	perSec  float64
	burst   float64
	buckets map[string]*bucket
	swept   time.Time
	now     func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

// newRateLimiter returns nil (no limit) when perMinute is not positive.
func newRateLimiter(perMinute, burst int) *rateLimiter {
	if perMinute <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = perMinute
	}
	return &rateLimiter{perSec: float64(perMinute) / 60, burst: float64(burst), buckets: map[string]*bucket{}, now: time.Now}
}

// allow takes one token for key, or reports how long until one is free.
func (l *rateLimiter) allow(key string) (bool, time.Duration) {
	if l == nil {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.Sub(l.swept) > time.Minute {
		for k, b := range l.buckets {
			if now.Sub(b.last) > 10*time.Minute {
				delete(l.buckets, k)
			}
		}
		l.swept = now
	}
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = math.Min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.perSec)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / l.perSec * float64(time.Second))
}

// limited writes a 429 if key is over its limit and reports whether it did.
func limited(w http.ResponseWriter, l *rateLimiter, key, what string) bool {
	ok, wait := l.allow(key)
	if ok {
		return false
	}
	secs := int(math.Ceil(wait.Seconds()))
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	writeError(w, http.StatusTooManyRequests, fmt.Sprintf("too many %s from your address; try again in %ds", what, secs))
	return true
}
