package queue

import (
	"context"
	"encoding/json"
	"time"
)

// StatusEvent is published on the events channel for every status change, and
// streamed to the dashboard over SSE.
type StatusEvent struct {
	JobID    string    `json:"job_id"`
	Type     string    `json:"type"`
	From     string    `json:"from"`
	To       string    `json:"to"`
	WorkerID string    `json:"worker_id,omitempty"`
	Error    string    `json:"error,omitempty"`
	Attempts int       `json:"attempts"`
	At       time.Time `json:"at"`
}

// PublishEvent broadcasts a status change. Best effort: the durable record is
// the job_events table, so a dropped publish only affects the live feed.
func (q *Queue) PublishEvent(ctx context.Context, ev StatusEvent) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return q.rdb.Publish(ctx, q.Keys.Events, b).Err()
}

// SubscribeEvents delivers status events until ctx is cancelled.
func (q *Queue) SubscribeEvents(ctx context.Context) <-chan StatusEvent {
	out := make(chan StatusEvent, 1024)
	sub := q.rdb.Subscribe(ctx, q.Keys.Events)
	go func() {
		defer close(out)
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				var ev StatusEvent
				if json.Unmarshal([]byte(m.Payload), &ev) != nil {
					continue
				}
				select {
				case out <- ev:
				default: // slow consumer: drop rather than stall Redis
				}
			}
		}
	}()
	return out
}

// ControlMessage is a command sent to workers.
type ControlMessage struct {
	Action   string `json:"action"` // "kill"
	WorkerID string `json:"worker_id"`
}

// PublishKill tells one worker to crash itself with os.Exit(1). Returns how
// many subscribers received it (0 means no worker by that ID is listening).
func (q *Queue) PublishKill(ctx context.Context, workerID string) (int64, error) {
	b, _ := json.Marshal(ControlMessage{Action: "kill", WorkerID: workerID})
	return q.rdb.Publish(ctx, q.Keys.Control, b).Result()
}

// SubscribeControl delivers control messages addressed to workerID. It
// returns only once Redis has confirmed the subscription, so a kill sent
// after it returns is guaranteed to arrive.
func (q *Queue) SubscribeControl(ctx context.Context, workerID string) (<-chan ControlMessage, error) {
	sub := q.rdb.Subscribe(ctx, q.Keys.Control)
	if _, err := sub.Receive(ctx); err != nil {
		sub.Close()
		return nil, err
	}
	out := make(chan ControlMessage, 4)
	go func() {
		defer close(out)
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				var cm ControlMessage
				if json.Unmarshal([]byte(m.Payload), &cm) != nil || cm.WorkerID != workerID {
					continue
				}
				out <- cm
			}
		}
	}()
	return out, nil
}
