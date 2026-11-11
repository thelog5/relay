// Package supervise runs a set of child processes and restarts the ones
// that exit. Relay uses it to run the api and several workers inside one
// container on hosts that give a service a single process (Render's free
// web service): each worker is still a real OS process, so the dashboard's
// kill button still ends one with os.Exit(1), and the supervisor plays the
// part that `restart: unless-stopped` plays in docker compose.
package supervise

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Proc describes one child process.
type Proc struct {
	Name string
	Path string
	Args []string
	// Env is appended to the supervisor's own environment.
	Env []string
	// Restart starts the process again whenever it exits. A process without
	// Restart is essential: when it exits, everything is shut down.
	Restart bool
}

// Supervisor starts Procs and keeps them running.
type Supervisor struct {
	Procs        []Proc
	RestartDelay time.Duration // default 1s
	StopGrace    time.Duration // SIGTERM to SIGKILL; default 20s
	Logger       *slog.Logger

	starts atomic.Int64
}

// Starts reports how many processes have been started, restarts included.
func (s *Supervisor) Starts() int64 { return s.starts.Load() }

// Run starts every process and blocks until ctx is cancelled or an essential
// process exits. Children get SIGTERM and StopGrace to finish; Run returns
// only after all of them have exited. The error names the essential process
// that exited, or is nil after a cancel.
func (s *Supervisor) Run(ctx context.Context) error {
	if s.RestartDelay <= 0 {
		s.RestartDelay = time.Second
	}
	if s.StopGrace <= 0 {
		s.StopGrace = 20 * time.Second
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	parent := ctx
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	for _, p := range s.Procs {
		wg.Go(func() { s.keep(ctx, cancel, p) })
	}
	wg.Wait()
	// A shutdown the caller asked for is not an error; an essential process
	// exiting on its own is.
	if parent.Err() == nil {
		return context.Cause(ctx)
	}
	return nil
}

func (s *Supervisor) keep(ctx context.Context, cancel context.CancelCauseFunc, p Proc) {
	for {
		err := s.runOnce(ctx, p)
		if ctx.Err() != nil {
			return
		}
		if !p.Restart {
			if err == nil {
				err = errors.New("exit status 0")
			}
			cancel(fmt.Errorf("%s exited: %w", p.Name, err))
			return
		}
		s.Logger.Warn("process exited; restarting", "name", p.Name, "err", err, "delay", s.RestartDelay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.RestartDelay):
		}
	}
}

func (s *Supervisor) runOnce(ctx context.Context, p Proc) error {
	cmd := exec.CommandContext(ctx, p.Path, p.Args...)
	cmd.Env = append(os.Environ(), p.Env...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	// On shutdown ask politely first so workers can finish or hand back their
	// jobs; WaitDelay escalates to SIGKILL.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = s.StopGrace
	if err := cmd.Start(); err != nil {
		return err
	}
	s.starts.Add(1)
	s.Logger.Info("process started", "name", p.Name, "pid", cmd.Process.Pid)
	return cmd.Wait()
}
