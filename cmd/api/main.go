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
	"strings"
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

	limits := demoLimits(config.Bool("DEMO_MODE", false))
	if limits.Active() {
		log.Info("public demo limits on", "jobs_per_min", limits.JobsPerMinute, "burst", limits.JobsBurst,
			"kills_per_min", limits.KillsPerMinute, "max_unfinished", limits.MaxUnfinished,
			"disabled_types", limits.DisabledTypes, "max_sleep_ms", limits.MaxSleepMS, "proxy_hops", limits.ProxyHops)
	}
	srv := api.New(api.Config{
		KillToken: config.String("KILL_TOKEN", ""),
		Limits:    limits,
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

	addr := config.String("HTTP_ADDR", "")
	if addr == "" {
		addr = ":" + config.String("PORT", "8080") // PaaS hosts pass the port in PORT
	}
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

// demoLimits returns the public-demo protections. DEMO_MODE=true turns on
// sensible defaults; each RATE_LIMIT_* / MAX_* / DISABLED_JOB_TYPES /
// TRUST_PROXY_HOPS variable overrides one of them either way.
func demoLimits(demo bool) api.Limits {
	var d api.Limits
	disabled := ""
	if demo {
		d = api.Limits{JobsPerMinute: 1200, JobsBurst: 600, KillsPerMinute: 12, MaxUnfinished: 20000, MaxSleepMS: 10000}
		disabled = "webhook"
	}
	l := api.Limits{
		JobsPerMinute:  config.Int("RATE_LIMIT_JOBS_PER_MIN", d.JobsPerMinute),
		JobsBurst:      config.Int("RATE_LIMIT_JOBS_BURST", d.JobsBurst),
		KillsPerMinute: config.Int("RATE_LIMIT_KILLS_PER_MIN", d.KillsPerMinute),
		MaxUnfinished:  int64(config.Int("MAX_UNFINISHED_JOBS", int(d.MaxUnfinished))),
		MaxSleepMS:     config.Int("MAX_SLEEP_MS", d.MaxSleepMS),
		ProxyHops:      config.Int("TRUST_PROXY_HOPS", 0),
	}
	for _, t := range strings.Split(config.String("DISABLED_JOB_TYPES", disabled), ",") {
		if t = strings.TrimSpace(t); t != "" {
			l.DisabledTypes = append(l.DisabledTypes, t)
		}
	}
	return l
}
