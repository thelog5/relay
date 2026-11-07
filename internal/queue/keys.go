// Package queue implements Relay's Redis side: the job stream and its
// consumer group, the delayed-retry sorted set, the dead-letter stream, the
// pub/sub channels for the dashboard and worker control, and the worker
// registry.
//
// Redis is the source of truth for what still needs to run. A job is always
// in exactly one of: the stream (unread or pending in a consumer's PEL), the
// retry set, or nowhere because it reached a terminal status in Postgres.
package queue

// Group is the consumer group every worker joins.
const Group = "workers"

// Keys holds every Redis key Relay uses, derived from one prefix so tests can
// run side by side.
type Keys struct {
	Prefix  string
	Jobs    string // stream, consumer group "workers": the main queue
	Retry   string // sorted set, score = run-at unix ms: delayed retries
	Dead    string // stream: dead-letter queue
	Events  string // pub/sub: status changes for the dashboard feed
	Control string // pub/sub: kill commands for workers
	Workers string // sorted set, score = last heartbeat unix ms
}

// NewKeys builds the key set for a prefix ("relay" in production).
func NewKeys(prefix string) Keys {
	if prefix == "" {
		prefix = "relay"
	}
	return Keys{
		Prefix:  prefix,
		Jobs:    prefix + ":jobs",
		Retry:   prefix + ":retry",
		Dead:    prefix + ":dead",
		Events:  prefix + ":events",
		Control: prefix + ":control",
		Workers: prefix + ":workers",
	}
}

// WorkerKey is the hash holding one worker's stats.
func (k Keys) WorkerKey(id string) string { return k.Prefix + ":worker:" + id }
