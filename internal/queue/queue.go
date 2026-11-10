package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Message is one delivery of a job from the stream.
type Message struct {
	ID    string // stream entry ID
	JobID string
}

// Queue wraps a Redis client with Relay's stream operations.
type Queue struct {
	rdb  *redis.Client
	Keys Keys
}

// New returns a Queue using the given client and key prefix.
func New(rdb *redis.Client, prefix string) *Queue {
	return &Queue{rdb: rdb, Keys: NewKeys(prefix)}
}

// Connect parses a redis:// URL, connects, and waits for Redis to answer.
func Connect(ctx context.Context, url string, poolSize int) (*redis.Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	if poolSize > 0 {
		opts.PoolSize = poolSize
	}
	rdb := redis.NewClient(opts)
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := rdb.Ping(ctx).Err()
		if err == nil {
			return rdb, nil
		}
		if time.Now().After(deadline) {
			rdb.Close()
			return nil, fmt.Errorf("redis not ready: %w", err)
		}
		select {
		case <-ctx.Done():
			rdb.Close()
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Client exposes the underlying Redis client.
func (q *Queue) Client() *redis.Client { return q.rdb }

// EnsureGroup creates the stream and the "workers" consumer group if needed.
func (q *Queue) EnsureGroup(ctx context.Context) error {
	err := q.rdb.XGroupCreateMkStream(ctx, q.Keys.Jobs, Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Enqueue appends a job to the stream.
func (q *Queue) Enqueue(ctx context.Context, jobID string) (string, error) {
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.Keys.Jobs,
		Values: map[string]any{"job_id": jobID},
	}).Result()
}

func toMessages(xms []redis.XMessage) []Message {
	out := make([]Message, 0, len(xms))
	for _, m := range xms {
		id, _ := m.Values["job_id"].(string)
		out = append(out, Message{ID: m.ID, JobID: id})
	}
	return out
}

// Read fetches up to count new messages for consumer, blocking up to block.
// Returns no messages and no error on timeout.
func (q *Queue) Read(ctx context.Context, consumer string, count int64, block time.Duration) ([]Message, error) {
	return q.read(ctx, consumer, ">", count, block)
}

// ReadOwnPending returns messages already delivered to consumer but never
// acknowledged. A worker that restarts under the same name calls it first.
func (q *Queue) ReadOwnPending(ctx context.Context, consumer string, count int64) ([]Message, error) {
	return q.read(ctx, consumer, "0", count, -1)
}

func (q *Queue) read(ctx context.Context, consumer, start string, count int64, block time.Duration) ([]Message, error) {
	res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    Group,
		Consumer: consumer,
		Streams:  []string{q.Keys.Jobs, start},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil && strings.Contains(err.Error(), "NOGROUP") {
		// Redis lost the stream or the group (a restart without
		// persistence, such as a free hosted instance). Recreate them and
		// let the caller read again; the reconciler re-enqueues any jobs
		// Postgres still has as unfinished.
		return nil, q.EnsureGroup(ctx)
	}
	if err != nil {
		return nil, err
	}
	var out []Message
	for _, s := range res {
		out = append(out, toMessages(s.Messages)...)
	}
	return out, nil
}

// Ack acknowledges a finished message and deletes it from the stream so the
// stream only ever holds outstanding work.
func (q *Queue) Ack(ctx context.Context, msgID string) error {
	_, err := q.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.XAck(ctx, q.Keys.Jobs, Group, msgID)
		p.XDel(ctx, q.Keys.Jobs, msgID)
		return nil
	})
	return err
}

// ScheduleRetry atomically parks the job in the retry set until runAt and
// removes the message from the stream.
func (q *Queue) ScheduleRetry(ctx context.Context, msg Message, runAt time.Time) error {
	return scheduleRetryScript.Run(ctx, q.rdb,
		[]string{q.Keys.Jobs, q.Keys.Retry},
		Group, msg.ID, runAt.UnixMilli(), msg.JobID).Err()
}

// DeadLetter atomically records the job on the dead-letter stream with its
// last error and removes the message from the main stream (R5).
func (q *Queue) DeadLetter(ctx context.Context, msg Message, lastErr string) error {
	_, err := q.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.XAdd(ctx, &redis.XAddArgs{
			Stream: q.Keys.Dead,
			Values: map[string]any{"job_id": msg.JobID, "error": lastErr, "at": time.Now().UTC().Format(time.RFC3339Nano)},
		})
		p.XAck(ctx, q.Keys.Jobs, Group, msg.ID)
		p.XDel(ctx, q.Keys.Jobs, msg.ID)
		return nil
	})
	return err
}

// AutoClaim takes over up to count messages that have sat unacknowledged
// for at least minIdle, from any consumer (R6). Workers call it every few
// seconds; a crashed worker's messages stop being heartbeated and go idle.
func (q *Queue) AutoClaim(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]Message, error) {
	msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   q.Keys.Jobs,
		Group:    Group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    count,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toMessages(msgs), nil
}

// ExtendLease resets the idle timer on a message this consumer still owns.
// Returns false if someone else has claimed it in the meantime.
func (q *Queue) ExtendLease(ctx context.Context, consumer, msgID string) (bool, error) {
	n, err := extendLeaseScript.Run(ctx, q.rdb, []string{q.Keys.Jobs}, Group, consumer, msgID).Int()
	return n == 1, err
}

// DueRetries lists job IDs whose retry time has passed.
func (q *Queue) DueRetries(ctx context.Context, now time.Time, limit int64) ([]string, error) {
	return q.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     q.Keys.Retry,
		Start:   "-inf",
		Stop:    strconv.FormatInt(now.UnixMilli(), 10),
		ByScore: true,
		Count:   limit,
	}).Result()
}

// PromoteRetries moves the given job IDs from the retry set onto the stream.
func (q *Queue) PromoteRetries(ctx context.Context, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return promoteRetriesScript.Run(ctx, q.rdb, []string{q.Keys.Retry, q.Keys.Jobs}, args...).Int()
}

// RemoveDead deletes a job's entries from the dead-letter stream (R12).
func (q *Queue) RemoveDead(ctx context.Context, jobID string) (int, error) {
	removed := 0
	start := "-"
	for {
		msgs, err := q.rdb.XRangeN(ctx, q.Keys.Dead, start, "+", 500).Result()
		if err != nil {
			return removed, err
		}
		for _, m := range msgs {
			if m.Values["job_id"] == jobID {
				if err := q.rdb.XDel(ctx, q.Keys.Dead, m.ID).Err(); err != nil {
					return removed, err
				}
				removed++
			}
		}
		if len(msgs) < 500 {
			return removed, nil
		}
		start = "(" + msgs[len(msgs)-1].ID
	}
}

// Outstanding reports which of ids are somewhere in Redis: on the stream
// (unread or pending) or in the retry set. Scans the stream, which only ever
// holds unfinished work because acked entries are deleted.
func (q *Queue) Outstanding(ctx context.Context, ids []string) (map[string]bool, error) {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = false
	}
	start := "-"
	for {
		msgs, err := q.rdb.XRangeN(ctx, q.Keys.Jobs, start, "+", 1000).Result()
		if err != nil {
			return nil, err
		}
		for _, m := range msgs {
			if id, ok := m.Values["job_id"].(string); ok {
				if _, tracked := want[id]; tracked {
					want[id] = true
				}
			}
		}
		if len(msgs) < 1000 {
			break
		}
		start = "(" + msgs[len(msgs)-1].ID
	}
	p := q.rdb.Pipeline()
	scores := make(map[string]*redis.FloatCmd, len(ids))
	for _, id := range ids {
		if !want[id] {
			scores[id] = p.ZScore(ctx, q.Keys.Retry, id)
		}
	}
	if len(scores) > 0 {
		if _, err := p.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		for id, c := range scores {
			if c.Err() == nil {
				want[id] = true
			}
		}
	}
	return want, nil
}

// Depth summarizes where outstanding work sits.
type Depth struct {
	Stream  int64 `json:"stream"`  // entries in the stream (unread + pending)
	Pending int64 `json:"pending"` // delivered, not yet acknowledged
	Backlog int64 `json:"backlog"` // waiting to be read: stream - pending
	Retry   int64 `json:"retry"`   // waiting for their backoff to expire
	Dead    int64 `json:"dead"`    // entries on the dead-letter stream
}

// Depth reads queue depth gauges in one round trip.
func (q *Queue) Depth(ctx context.Context) (Depth, error) {
	var d Depth
	p := q.rdb.Pipeline()
	xlen := p.XLen(ctx, q.Keys.Jobs)
	pend := p.XPending(ctx, q.Keys.Jobs, Group)
	zc := p.ZCard(ctx, q.Keys.Retry)
	dl := p.XLen(ctx, q.Keys.Dead)
	_, err := p.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) && !strings.Contains(err.Error(), "NOGROUP") {
		return d, err
	}
	d.Stream = xlen.Val()
	if pi := pend.Val(); pi != nil {
		d.Pending = pi.Count
	}
	d.Backlog = d.Stream - d.Pending
	if d.Backlog < 0 {
		d.Backlog = 0
	}
	d.Retry = zc.Val()
	d.Dead = dl.Val()
	return d, nil
}

// PendingByConsumer reports how many unacknowledged messages each consumer
// holds. Used by tests and the crash demo to see who is holding what.
func (q *Queue) PendingByConsumer(ctx context.Context) (map[string]int64, error) {
	pi, err := q.rdb.XPending(ctx, q.Keys.Jobs, Group).Result()
	if err != nil {
		return nil, err
	}
	return pi.Consumers, nil
}

// PruneConsumers deletes consumers that hold nothing and have been idle for
// longer than idle, so crashed workers do not pile up in XINFO forever.
func (q *Queue) PruneConsumers(ctx context.Context, idle time.Duration) (int, error) {
	cs, err := q.rdb.XInfoConsumers(ctx, q.Keys.Jobs, Group).Result()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, c := range cs {
		if c.Pending == 0 && c.Idle > idle {
			if err := q.rdb.XGroupDelConsumer(ctx, q.Keys.Jobs, Group, c.Name).Err(); err == nil {
				n++
			}
		}
	}
	return n, nil
}
