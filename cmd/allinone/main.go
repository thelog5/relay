// Command allinone runs the Relay api and WORKERS workers in one container,
// for hosts that run one process per service (Render's free web service).
// Every worker is a separate OS process, so killing one from the dashboard
// still orphans its jobs for real; the supervisor starts a replacement a
// second later, the way docker compose's restart policy does locally.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/thelog5/relay/internal/config"
	"github.com/thelog5/relay/internal/logging"
	"github.com/thelog5/relay/internal/supervise"
)

func main() {
	log := logging.Setup("allinone")
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	apiPath, err := exec.LookPath(config.String("API_BIN", "api"))
	if err != nil {
		return err
	}
	workerPath, err := exec.LookPath(config.String("WORKER_BIN", "worker"))
	if err != nil {
		return err
	}
	// Render and most PaaS hosts say which port to listen on via PORT.
	addr := config.String("HTTP_ADDR", "")
	if addr == "" {
		addr = ":" + config.String("PORT", "8080")
	}
	procs := []supervise.Proc{{Name: "api", Path: apiPath, Env: []string{"HTTP_ADDR=" + addr}}}
	n := config.Int("WORKERS", 3)
	for i := 1; i <= n; i++ {
		procs = append(procs, supervise.Proc{
			Name:    fmt.Sprintf("worker-%d", i),
			Path:    workerPath,
			Env:     []string{fmt.Sprintf("METRICS_ADDR=127.0.0.1:%d", 9090+i)},
			Restart: true,
		})
	}
	log.Info("starting", "addr", addr, "workers", n)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return (&supervise.Supervisor{Procs: procs, Logger: log}).Run(ctx)
}
