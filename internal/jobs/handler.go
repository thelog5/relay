package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// Handler runs one job type. Handlers must be idempotent: Relay delivers
// at-least-once, so a crash can make the same job run twice.
type Handler interface {
	// Validate checks a payload at enqueue time so bad input is rejected with
	// a 400 instead of burning retries.
	Validate(payload json.RawMessage) error
	// Handle does the work. A returned error schedules a retry unless it is
	// wrapped with Permanent, in which case the job goes straight to dead.
	Handle(ctx context.Context, job *Job) error
}

// Registry maps a job type name to its handler.
type Registry map[string]Handler

// DefaultRegistry returns the three built-in job types from R2.
func DefaultRegistry() Registry {
	return Registry{
		"sleep":   SleepHandler{},
		"flaky":   FlakyHandler{},
		"webhook": NewWebhookHandler(),
	}
}

// Lookup returns the handler for typ.
func (r Registry) Lookup(typ string) (Handler, error) {
	h, ok := r[typ]
	if !ok {
		return nil, fmt.Errorf("unknown job type %q (known: %v)", typ, r.Types())
	}
	return h, nil
}

// Types lists registered job types, sorted.
func (r Registry) Types() []string {
	out := make([]string, 0, len(r))
	for k := range r {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// permanentError marks a failure that retrying cannot fix.
type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent wraps err so the worker moves the job to dead without retrying.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) is permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// decode unmarshals a payload strictly, so typos in field names are caught.
func decode(payload json.RawMessage, v any) error {
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytesReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}
	return nil
}
