// Command worker runs Relay jobs. Run several of these; each one is a
// separate process with WORKER_CONCURRENCY goroutines (R3).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thelog5/relay/internal/config"
	"github.com/thelog5/relay/internal/jobs"
	"github.com/thelog5/relay/internal/logging"
	"github.com/thelog5/relay/internal/metrics"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/store"
	"github.com/thelog5/relay/internal/worker"
)

func main() {
	log := logging.Setup("worker")
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	concurrency := config.Int("WORKER_CONCURRENCY", 8)

	st, err := store.Open(ctx, config.String("DATABASE_URL", "postgres://relay:relay@localhost:5432/relay?sslmode=disable"), int32(config.Int("PG_MAX_CONNS", concurrency+4)))
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer st.Close()
	// Migrations take an advisory lock, so racing the api here is safe and
	// means a worker never starts against a missing schema.
	if err := st.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// One blocking reader, one reclaimer, heartbeats, lease renewals and
	// pub/sub, plus a connection per running job.
	rdb, err := queue.Connect(ctx, config.String("REDIS_URL", "redis://localhost:6379/0"), config.Int("WORKER_REDIS_POOL_SIZE", concurrency*2+10))
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer rdb.Close()
	q := queue.New(rdb, config.String("RELAY_KEY_PREFIX", "relay"))

	w := worker.New(worker.Config{
		ID:                config.String("WORKER_ID", ""),
		Concurrency:       concurrency,
		ReclaimInterval:   config.Duration("RECLAIM_INTERVAL", 5*time.Second),
		ReclaimIdle:       config.Duration("RECLAIM_IDLE", 10*time.Second),
		LeaseInterval:     config.Duration("LEASE_INTERVAL", 3*time.Second),
		HeartbeatInterval: config.Duration("HEARTBEAT_INTERVAL", 2*time.Second),
		DeadAfter:         config.Duration("DEAD_AFTER", 6*time.Second),
		JobTimeout:        config.Duration("JOB_TIMEOUT", 60*time.Second),
		ShutdownGrace:     config.Duration("SHUTDOWN_GRACE", 20*time.Second),
		Logger:            log,
	}, st, q, jobs.DefaultRegistry())

	// Each worker exposes its own metrics for Prometheus to scrape.
	addr := config.String("METRICS_ADDR", ":9091")
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok\n"))
	})
	msrv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := msrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Warn("metrics server", "err", err)
		}
	}()

	if err := w.Run(ctx); err != nil {
		return fmt.Errorf("worker stopped: %w", err)
	}
	return msrv.Close()
}
