// Command relayctl drives a running Relay over HTTP. It enqueues jobs and runs
// the PRD's acceptance checks against a live stack:
//
//	relayctl enqueue -n 100 -type sleep -payload '{"ms":250}'
//	relayctl soak    -n 10000 -fail-rate 0.3   # every job ends succeeded or dead
//	relayctl crash                             # kill one worker mid-job, time recovery
//	relayctl chaos   -n 2000                   # kill every worker, confirm zero loss
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: relayctl <command> [flags]

commands:
  enqueue   enqueue N jobs of one type
  soak      enqueue N flaky jobs and verify every one ends succeeded or dead
  crash     kill one worker mid-job and time how long its jobs take to finish
  chaos     kill every worker at once and verify zero jobs are lost

common flags:
  -url      Relay API base URL (default $RELAY_URL or http://localhost:8080)
  -token    kill token if the API sets KILL_TOKEN (default $KILL_TOKEN)`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "enqueue":
		err = cmdEnqueue(args)
	case "soak":
		err = cmdSoak(args)
	case "crash":
		err = cmdCrash(args)
	case "chaos":
		err = cmdChaos(args)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}

// ---- client ---------------------------------------------------------------

type client struct {
	base  string
	token string
	http  *http.Client
}

func commonFlags(fs *flag.FlagSet) *client {
	c := &client{http: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 256}}}
	def := os.Getenv("RELAY_URL")
	if def == "" {
		def = "http://localhost:8080"
	}
	fs.StringVar(&c.base, "url", def, "Relay API base URL")
	fs.StringVar(&c.token, "token", os.Getenv("KILL_TOKEN"), "kill token")
	return c
}

func (c *client) do(method, path string, body, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, strings.TrimRight(c.base, "/")+path, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil && !errors.Is(err, io.EOF) {
			return resp.StatusCode, err
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return resp.StatusCode, nil
}

type jobReq struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts,omitempty"`
}

type job struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Status      string    `json:"status"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	WorkerID    string    `json:"worker_id"`
	UpdatedAt   time.Time `json:"updated_at"`
	Events      []struct {
		From string    `json:"from_status"`
		To   string    `json:"to_status"`
		At   time.Time `json:"at"`
	} `json:"events"`
}

// reclaimGaps returns, for each time this job was taken over from a crashed
// worker, how long it sat between starting on the dead worker and restarting
// on a live one (from job_events timestamps).
func (j job) reclaimGaps() []time.Duration {
	var gaps []time.Duration
	var lastRun time.Time
	for _, e := range j.Events {
		if e.To != "running" {
			continue
		}
		if e.From == "running" && !lastRun.IsZero() {
			gaps = append(gaps, e.At.Sub(lastRun))
		}
		lastRun = e.At
	}
	return gaps
}

type worker struct {
	ID       string `json:"id"`
	Alive    bool   `json:"alive"`
	InFlight int64  `json:"in_flight"`
}

type stats struct {
	Statuses map[string]int64 `json:"statuses"`
	Total    int64            `json:"total"`
	Workers  []worker         `json:"workers"`
	Recovery struct {
		Reclaimed int64   `json:"reclaimed"`
		Avg       float64 `json:"avg_reclaim_secs"`
		Max       float64 `json:"max_reclaim_secs"`
	} `json:"recovery"`
}

func (s stats) open() int64 {
	return s.Statuses["queued"] + s.Statuses["running"] + s.Statuses["retry_scheduled"]
}

func (c *client) stats() (stats, error) {
	var s stats
	code, err := c.do("GET", "/stats", nil, &s)
	if err == nil && code != 200 {
		err = fmt.Errorf("GET /stats: %d", code)
	}
	return s, err
}

func (c *client) getJob(id string) (job, error) {
	var j job
	code, err := c.do("GET", "/jobs/"+id, nil, &j)
	if err == nil && code != 200 {
		err = fmt.Errorf("GET /jobs/%s: %d", id, code)
	}
	return j, err
}

func (c *client) kill(id string) error {
	path := "/workers/" + id + "/kill"
	if c.token != "" {
		path += "?token=" + c.token
	}
	code, err := c.do("POST", path, nil, nil)
	if err == nil && code != http.StatusAccepted {
		err = fmt.Errorf("kill %s: HTTP %d", id, code)
	}
	return err
}

// enqueueMany posts n copies of req with `conc` concurrent clients and
// returns the accepted job IDs.
func (c *client) enqueueMany(req jobReq, n, conc int) ([]string, time.Duration, error) {
	ids := make([]string, n)
	var next, failed atomic.Int64
	var firstErr atomic.Value
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if i >= int64(n) {
					return
				}
				var out struct {
					ID string `json:"id"`
				}
				code, err := c.do("POST", "/jobs", req, &out)
				if err == nil && code != http.StatusAccepted {
					err = fmt.Errorf("POST /jobs: HTTP %d", code)
				}
				if err != nil {
					failed.Add(1)
					firstErr.CompareAndSwap(nil, err)
					continue
				}
				ids[i] = out.ID
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if f := failed.Load(); f > 0 {
		return ids, elapsed, fmt.Errorf("%d of %d enqueues failed; first: %v", f, n, firstErr.Load())
	}
	return ids, elapsed, nil
}

// waitSettled polls /stats until no job is queued, running or waiting to
// retry, and at least `want` jobs exist in total.
func (c *client) waitSettled(want int64, timeout time.Duration, progress bool) (stats, error) {
	deadline := time.Now().Add(timeout)
	last := time.Time{}
	for {
		s, err := c.stats()
		if err == nil && s.Total >= want && s.open() == 0 {
			return s, nil
		}
		if progress && time.Since(last) > 2*time.Second && err == nil {
			fmt.Printf("  … queued=%d running=%d retry=%d succeeded=%d dead=%d\n",
				s.Statuses["queued"], s.Statuses["running"], s.Statuses["retry_scheduled"], s.Statuses["succeeded"], s.Statuses["dead"])
			last = time.Now()
		}
		if time.Now().After(deadline) {
			return s, fmt.Errorf("not settled after %s: %v", timeout, s.Statuses)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// ---- commands -------------------------------------------------------------

func cmdEnqueue(args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	c := commonFlags(fs)
	n := fs.Int("n", 100, "number of jobs")
	typ := fs.String("type", "sleep", "job type")
	payload := fs.String("payload", `{"ms":250}`, "JSON payload")
	maxAttempts := fs.Int("max-attempts", 0, "max attempts (0 = server default)")
	conc := fs.Int("c", 32, "concurrent HTTP clients")
	fs.Parse(args)
	ids, took, err := c.enqueueMany(jobReq{Type: *typ, Payload: json.RawMessage(*payload), MaxAttempts: *maxAttempts}, *n, *conc)
	fmt.Printf("enqueued %d %s jobs in %s (%.0f/s)\n", countNonEmpty(ids), *typ, took.Round(time.Millisecond), float64(*n)/took.Seconds())
	return err
}

func cmdSoak(args []string) error {
	fs := flag.NewFlagSet("soak", flag.ExitOnError)
	c := commonFlags(fs)
	n := fs.Int("n", 10000, "number of flaky jobs")
	rate := fs.Float64("fail-rate", 0.3, "per-attempt failure probability")
	conc := fs.Int("c", 64, "concurrent HTTP clients")
	timeout := fs.Duration("timeout", 10*time.Minute, "how long to wait for every job to settle")
	fs.Parse(args)

	before, err := c.stats()
	if err != nil {
		return err
	}
	fmt.Printf("soak: enqueueing %d flaky jobs at fail_rate=%.2f\n", *n, *rate)
	payload := json.RawMessage(fmt.Sprintf(`{"fail_rate":%g}`, *rate))
	start := time.Now()
	ids, took, err := c.enqueueMany(jobReq{Type: "flaky", Payload: payload}, *n, *conc)
	if err != nil {
		return err
	}
	fmt.Printf("  enqueued in %s (%.0f/s)\n", took.Round(time.Millisecond), float64(*n)/took.Seconds())

	after, err := c.waitSettled(before.Total+int64(*n), *timeout, true)
	if err != nil {
		return err
	}
	total := time.Since(start)
	succ := after.Statuses["succeeded"] - before.Statuses["succeeded"]
	dead := after.Statuses["dead"] - before.Statuses["dead"]
	fmt.Printf("\nsoak result (%s)\n", total.Round(time.Millisecond))
	fmt.Printf("  sent       %d\n  succeeded  %d\n  dead       %d (expected ≈ %.0f with 5 attempts)\n  stuck      %d\n",
		len(ids), succ, dead, float64(*n)*pow(*rate, 5), after.open())
	if succ+dead != int64(*n) {
		return fmt.Errorf("succeeded+dead = %d, want %d", succ+dead, *n)
	}
	fmt.Println("PASS: every job ended as succeeded or dead, none stuck")
	return nil
}

func cmdCrash(args []string) error {
	fs := flag.NewFlagSet("crash", flag.ExitOnError)
	c := commonFlags(fs)
	n := fs.Int("n", 60, "sleep jobs to enqueue")
	ms := fs.Int("ms", 2000, "duration of each sleep job")
	limit := fs.Duration("limit", 15*time.Second, "max time from kill to the last orphaned job finishing")
	fs.Parse(args)

	ids, _, err := c.enqueueMany(jobReq{Type: "sleep", Payload: json.RawMessage(fmt.Sprintf(`{"ms":%d}`, *ms))}, *n, 16)
	if err != nil {
		return err
	}
	// Wait for a worker to be mid-job.
	var victim string
	deadline := time.Now().Add(10 * time.Second)
	for victim == "" {
		s, err := c.stats()
		if err != nil {
			return err
		}
		for _, w := range s.Workers {
			if w.Alive && w.InFlight > 0 {
				victim = w.ID
				break
			}
		}
		if time.Now().After(deadline) {
			return errors.New("no worker picked up jobs")
		}
		time.Sleep(100 * time.Millisecond)
	}
	held := heldBy(c, ids, victim)
	if len(held) == 0 {
		return fmt.Errorf("worker %s holds no jobs from this run", victim)
	}
	fmt.Printf("crash: killing %s while it runs %d jobs\n", victim, len(held))
	killedAt := time.Now()
	if err := c.kill(victim); err != nil {
		return err
	}

	worst := time.Duration(0)
	for _, id := range held {
		for {
			j, err := c.getJob(id)
			if err == nil && (j.Status == "succeeded" || j.Status == "dead") {
				took := time.Since(killedAt)
				if took > worst {
					worst = took
				}
				if j.WorkerID == victim {
					return fmt.Errorf("job %s finished on the killed worker", id)
				}
				fmt.Printf("  %s %-9s on %s after %s\n", id[:8], j.Status, j.WorkerID, took.Round(10*time.Millisecond))
				break
			}
			if time.Since(killedAt) > 2*(*limit) {
				return fmt.Errorf("job %s still %s after %s", id, j.Status, time.Since(killedAt).Round(time.Second))
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	fmt.Printf("\ncrash result: %d orphaned jobs, last finished %s after the kill (limit %s)\n", len(held), worst.Round(10*time.Millisecond), *limit)
	if worst > *limit {
		return fmt.Errorf("recovery took %s, over %s", worst, *limit)
	}
	fmt.Println("PASS: every job held by the killed worker finished on another worker in time")
	return nil
}

// heldBy returns which of ids are currently running on worker.
func heldBy(c *client, ids []string, worker string) []string {
	var held []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for _, id := range ids {
		if id == "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(id string) {
			defer wg.Done()
			defer func() { <-sem }()
			j, err := c.getJob(id)
			if err == nil && j.Status == "running" && j.WorkerID == worker {
				mu.Lock()
				held = append(held, id)
				mu.Unlock()
			}
		}(id)
	}
	wg.Wait()
	sort.Strings(held)
	return held
}

func cmdChaos(args []string) error {
	fs := flag.NewFlagSet("chaos", flag.ExitOnError)
	c := commonFlags(fs)
	n := fs.Int("n", 2000, "sleep jobs to enqueue")
	ms := fs.Int("ms", 200, "duration of each sleep job")
	timeout := fs.Duration("timeout", 3*time.Minute, "how long to wait for every job to finish after the kill")
	fs.Parse(args)

	before, err := c.stats()
	if err != nil {
		return err
	}
	ids, _, err := c.enqueueMany(jobReq{Type: "sleep", Payload: json.RawMessage(fmt.Sprintf(`{"ms":%d}`, *ms))}, *n, 32)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	s, err := c.stats()
	if err != nil {
		return err
	}
	var killed []string
	for _, w := range s.Workers {
		if w.Alive {
			if err := c.kill(w.ID); err != nil {
				return err
			}
			killed = append(killed, w.ID)
		}
	}
	fmt.Printf("chaos: killed all %d workers with %d jobs running and %d queued\n", len(killed), s.Statuses["running"], s.Statuses["queued"])
	fmt.Println("  waiting for workers to come back (docker restart: unless-stopped) and drain…")

	after, err := c.waitSettled(before.Total+int64(*n), *timeout, true)
	if err != nil {
		return err
	}
	// Check every job individually: the count of jobs in Postgres vs jobs sent.
	lost := 0
	var gaps []time.Duration
	for _, id := range ids {
		j, err := c.getJob(id)
		if err != nil || j.Status != "succeeded" {
			lost++
			continue
		}
		gaps = append(gaps, j.reclaimGaps()...)
	}
	avg, worst := summarize(gaps)
	fmt.Printf("\nchaos result\n  sent       %d\n  succeeded  %d\n  lost       %d\n  rescued    %d (avg gap %s, worst %s)\n",
		len(ids), after.Statuses["succeeded"]-before.Statuses["succeeded"], lost, len(gaps), avg, worst)
	if lost > 0 {
		return fmt.Errorf("%d jobs did not succeed", lost)
	}
	fmt.Println("PASS: zero jobs lost after killing every worker")
	return nil
}

func summarize(ds []time.Duration) (avg, worst time.Duration) {
	if len(ds) == 0 {
		return 0, 0
	}
	var sum time.Duration
	for _, d := range ds {
		sum += d
		worst = max(worst, d)
	}
	return (sum / time.Duration(len(ds))).Round(10 * time.Millisecond), worst.Round(10 * time.Millisecond)
}

func countNonEmpty(ids []string) int {
	n := 0
	for _, id := range ids {
		if id != "" {
			n++
		}
	}
	return n
}

func pow(x float64, n int) float64 {
	r := 1.0
	for i := 0; i < n; i++ {
		r *= x
	}
	return r
}

