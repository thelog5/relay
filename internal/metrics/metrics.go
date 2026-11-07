// Package metrics defines Relay's Prometheus metrics (R9). Each process (api
// and every worker) exposes its own /metrics; Prometheus scrapes them all.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Outcome label values for job completion.
const (
	OutcomeSucceeded = "succeeded"
	OutcomeRetried   = "retried"
	OutcomeDead      = "dead"
)

var (
	// API side.
	JobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_jobs_enqueued_total",
		Help: "Jobs accepted by POST /jobs.",
	}, []string{"type"})

	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_http_requests_total",
		Help: "HTTP requests served by the api, by route and status code.",
	}, []string{"route", "code"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "relay_http_request_duration_seconds",
		Help:    "HTTP request latency by route.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"route"})

	RetriesPromoted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_retries_promoted_total",
		Help: "Delayed retries moved back onto the stream by the scheduler.",
	})

	JobsReconciled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_jobs_reconciled_total",
		Help: "Queued jobs found missing from Redis and put back on the stream.",
	})

	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "relay_queue_depth",
		Help: "Outstanding work by where it sits: backlog, pending, retry, dead.",
	}, []string{"state"})

	JobsByStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "relay_jobs_by_status",
		Help: "Jobs in Postgres by current status.",
	}, []string{"status"})

	WorkersAlive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "relay_workers_alive",
		Help: "Workers that heartbeated recently.",
	})

	WorkerKills = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_worker_kills_total",
		Help: "Kill commands sent from the dashboard.",
	})

	// Worker side.
	JobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_jobs_completed_total",
		Help: "Job attempts finished, by type and outcome (succeeded, retried, dead).",
	}, []string{"type", "outcome"})

	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "relay_job_duration_seconds",
		Help:    "Time spent in the job handler, by type and outcome.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2.5, 12),
	}, []string{"type", "outcome"})

	JobsReclaimed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_jobs_reclaimed_total",
		Help: "Jobs this worker took over from a crashed worker via XAUTOCLAIM.",
	})

	JobsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "relay_jobs_in_flight",
		Help: "Jobs currently executing in this worker.",
	})

	DuplicateDeliveries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_duplicate_deliveries_total",
		Help: "Deliveries skipped because the job had already finished.",
	})

	LeaseLost = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_lease_lost_total",
		Help: "Jobs whose result was discarded because another worker reclaimed them first.",
	})
)

// Handler serves the default registry in Prometheus text format.
func Handler() http.Handler { return promhttp.Handler() }
