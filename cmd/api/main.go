// Command api runs Relay's HTTP server and the retry scheduler.
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

	"github.com/thelog5/relay/internal/api"
	"github.com/thelog5/relay/internal/config"
	"github.com/thelog5/relay/internal/jobs"
	"github.com/thelog5/relay/internal/logging"
	"github.com/thelog5/relay/internal/queue"
	"github.com/thelog5/relay/internal/scheduler"
	"github.com/thelog5/relay/internal/store"
)

func main() {
	log := logging.Setup("api")
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, config.String("DATABASE_URL", "postgres://relay:relay@localhost:5432/relay?sslmode=disable"), int32(config.Int("PG_MAX_CONNS", 20)))
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	rdb, err := queue.Connect(ctx, config.String("REDIS_URL", "redis://localhost:6379/0"), config.Int("REDIS_POOL_SIZE", 50))
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer rdb.Close()
	q := queue.New(rdb, config.String("RELAY_KEY_PREFIX", "relay"))
	if err := q.EnsureGroup(ctx); err != nil {
		return fmt.Errorf("consumer group: %w", err)
	}

	srv := api.New(api.Config{
		KillToken: config.String("KILL_TOKEN", ""),
		Logger:    log,
	}, st, q, jobs.DefaultRegistry())

	sched := &scheduler.Scheduler{
		Store:    st,
		Queue:    q,
		Interval: config.Duration("SCHEDULER_INTERVAL", 500*time.Millisecond),
		Logger:   log.With("component", "scheduler"),
	}
	go sched.Run(ctx)
	go (&scheduler.Reconciler{
		Store:    st,
		Queue:    q,
		Interval: config.Duration("RECONCILE_INTERVAL", 30*time.Second),
		Grace:    config.Duration("RECONCILE_GRACE", 30*time.Second),
		Logger:   log.With("component", "reconciler"),
	}).Run(ctx)
	go srv.RunHub(ctx)

	addr := config.String("HTTP_ADDR", ":8080")
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("api listening", "addr", addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpSrv.Shutdown(shutdownCtx)
}
