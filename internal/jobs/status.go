// Package jobs defines the job model, its lifecycle, the retry policy and the
// built-in job handlers.
package jobs

import "fmt"

// Status is the lifecycle state of a job.
//
//	queued -> running -> succeeded
//	running -> retry_scheduled -> queued
//	running -> dead (after max attempts)
//
// A job can also go running -> running when a live worker reclaims it from a
// crashed one.
type Status string

const (
	StatusQueued         Status = "queued"
	StatusRunning        Status = "running"
	StatusSucceeded      Status = "succeeded"
	StatusRetryScheduled Status = "retry_scheduled"
	StatusDead           Status = "dead"
)

// AllStatuses lists every status in lifecycle order. Handy for dashboards and
// for zero-filling count maps.
var AllStatuses = []Status{
	StatusQueued,
	StatusRunning,
	StatusRetryScheduled,
	StatusSucceeded,
	StatusDead,
}

// transitions is the allowed state machine. The empty status is the "before
// the job existed" state used for the very first event.
var transitions = map[Status][]Status{
	"": {StatusQueued},
	// queued -> dead only happens when the job row was written but the
	// XADD to Redis failed, so the job is failed loudly instead of stuck.
	StatusQueued:         {StatusRunning, StatusDead},
	StatusRunning:        {StatusSucceeded, StatusRetryScheduled, StatusDead, StatusRunning},
	StatusRetryScheduled: {StatusQueued},
	// A dead job can be manually retried from the dashboard (R12).
	StatusDead: {StatusQueued},
}

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	for _, v := range AllStatuses {
		if v == s {
			return true
		}
	}
	return false
}

// Terminal reports whether no further automatic transitions happen from s.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusDead
}

// CanTransition reports whether from -> to is an allowed lifecycle step.
func CanTransition(from, to Status) bool {
	for _, next := range transitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

// ParseStatus converts a string to a Status, rejecting unknown values.
func ParseStatus(s string) (Status, error) {
	st := Status(s)
	if !st.Valid() {
		return "", fmt.Errorf("unknown status %q", s)
	}
	return st, nil
}
