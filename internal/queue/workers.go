package queue

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// WorkerInfo is one worker's registry entry, shown on the dashboard.
type WorkerInfo struct {
	ID          string    `json:"id"`
	Host        string    `json:"host"`
	PID         int       `json:"pid"`
	Concurrency int       `json:"concurrency"`
	StartedAt   time.Time `json:"started_at"`
	LastSeen    time.Time `json:"last_seen"`
	Alive       bool      `json:"alive"`
	InFlight    int64     `json:"in_flight"`
	Succeeded   int64     `json:"succeeded"`
	Failed      int64     `json:"failed"`
	Dead        int64     `json:"dead"`
	Reclaimed   int64     `json:"reclaimed"`
}

// Stat fields kept in each worker's hash.
const (
	StatInFlight  = "in_flight"
	StatSucceeded = "succeeded"
	StatFailed    = "failed"
	StatDead      = "dead"
	StatReclaimed = "reclaimed"
)

// workerTTL is how long a worker's hash survives without heartbeats. Long
// enough for the dashboard to show a killed worker as dead for a while.
const workerTTL = 10 * time.Minute

// Heartbeat records that a worker is alive, with its static info.
func (q *Queue) Heartbeat(ctx context.Context, w WorkerInfo) error {
	now := time.Now()
	key := q.Keys.WorkerKey(w.ID)
	_, err := q.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZAdd(ctx, q.Keys.Workers, redis.Z{Score: float64(now.UnixMilli()), Member: w.ID})
		p.HSet(ctx, key,
			"host", w.Host,
			"pid", w.PID,
			"concurrency", w.Concurrency,
			"started_at", w.StartedAt.UnixMilli(),
		)
		p.Expire(ctx, key, workerTTL)
		return nil
	})
	return err
}

// IncrWorkerStat adds delta to one of a worker's counters.
func (q *Queue) IncrWorkerStat(ctx context.Context, workerID, field string, delta int64) error {
	return q.rdb.HIncrBy(ctx, q.Keys.WorkerKey(workerID), field, delta).Err()
}

// SetWorkerStat overwrites one of a worker's counters.
func (q *Queue) SetWorkerStat(ctx context.Context, workerID, field string, v int64) error {
	return q.rdb.HSet(ctx, q.Keys.WorkerKey(workerID), field, v).Err()
}

// Workers lists workers seen within the last `recent`, marking those seen
// within `alive` as alive. Sorted by start time so the dashboard is stable.
func (q *Queue) Workers(ctx context.Context, alive, recent time.Duration) ([]WorkerInfo, error) {
	now := time.Now()
	// Forget workers we have not heard from in a long time.
	q.rdb.ZRemRangeByScore(ctx, q.Keys.Workers, "-inf", strconv.FormatInt(now.Add(-workerTTL).UnixMilli(), 10))

	zs, err := q.rdb.ZRangeByScoreWithScores(ctx, q.Keys.Workers, &redis.ZRangeBy{
		Min: strconv.FormatInt(now.Add(-recent).UnixMilli(), 10),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, err
	}
	p := q.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(zs))
	for i, z := range zs {
		cmds[i] = p.HGetAll(ctx, q.Keys.WorkerKey(z.Member.(string)))
	}
	if len(zs) > 0 {
		if _, err := p.Exec(ctx); err != nil {
			return nil, err
		}
	}
	out := make([]WorkerInfo, 0, len(zs))
	for i, z := range zs {
		h := cmds[i].Val()
		lastSeen := time.UnixMilli(int64(z.Score))
		w := WorkerInfo{
			ID:          z.Member.(string),
			Host:        h["host"],
			PID:         atoi(h["pid"]),
			Concurrency: atoi(h["concurrency"]),
			StartedAt:   time.UnixMilli(atoi64(h["started_at"])),
			LastSeen:    lastSeen,
			Alive:       now.Sub(lastSeen) <= alive,
			InFlight:    atoi64(h[StatInFlight]),
			Succeeded:   atoi64(h[StatSucceeded]),
			Failed:      atoi64(h[StatFailed]),
			Dead:        atoi64(h[StatDead]),
			Reclaimed:   atoi64(h[StatReclaimed]),
		}
		if !w.Alive {
			w.InFlight = 0
		}
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Alive != out[j].Alive {
			return out[i].Alive
		}
		return out[i].StartedAt.Before(out[j].StartedAt)
	})
	return out, nil
}

func atoi(s string) int     { n, _ := strconv.Atoi(s); return n }
func atoi64(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }

// DeadConsumers returns registered workers whose last heartbeat is older than
// deadAfter but which still hold pending messages. These are crashed workers
// whose jobs can be taken over immediately, without waiting for XAUTOCLAIM's
// idle threshold.
func (q *Queue) DeadConsumers(ctx context.Context, deadAfter time.Duration) ([]string, error) {
	pending, err := q.PendingByConsumer(ctx)
	if err != nil || len(pending) == 0 {
		return nil, err
	}
	cutoff := time.Now().Add(-deadAfter).UnixMilli()
	var dead []string
	for consumer, n := range pending {
		if n == 0 {
			continue
		}
		score, err := q.rdb.ZScore(ctx, q.Keys.Workers, consumer).Result()
		if err != nil {
			// Never registered (or long forgotten): leave it to XAUTOCLAIM.
			continue
		}
		if int64(score) < cutoff {
			dead = append(dead, consumer)
		}
	}
	return dead, nil
}

// ClaimFrom takes up to count of one consumer's pending messages that have
// been idle at least minIdle. XCLAIM re-checks idle time atomically, so a
// message the owner renewed in the meantime is left alone.
func (q *Queue) ClaimFrom(ctx context.Context, consumer, from string, minIdle time.Duration, count int64) ([]Message, error) {
	ps, err := q.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   q.Keys.Jobs,
		Group:    Group,
		Idle:     minIdle,
		Start:    "-",
		End:      "+",
		Count:    count,
		Consumer: from,
	}).Result()
	if err != nil || len(ps) == 0 {
		return nil, err
	}
	ids := make([]string, len(ps))
	for i, p := range ps {
		ids[i] = p.ID
	}
	msgs, err := q.rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   q.Keys.Jobs,
		Group:    Group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		return nil, err
	}
	return toMessages(msgs), nil
}
