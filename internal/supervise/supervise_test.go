package supervise

import (
	"context"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

func sh(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("needs a POSIX shell")
	}
	p, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh on PATH")
	}
	return p
}

func TestRestartsProcessesThatExit(t *testing.T) {
	s := &Supervisor{
		Procs:        []Proc{{Name: "crashy", Path: sh(t), Args: []string{"-c", "exit 1"}, Restart: true}},
		RestartDelay: 20 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if s.Starts() < 3 {
		t.Fatalf("started %d times in 400ms with a 20ms restart delay", s.Starts())
	}
}

func TestEssentialExitStopsEverything(t *testing.T) {
	s := &Supervisor{Procs: []Proc{
		{Name: "api", Path: sh(t), Args: []string{"-c", "sleep 0.1; exit 3"}},
		{Name: "worker", Path: sh(t), Args: []string{"-c", "sleep 30"}, Restart: true},
	}, StopGrace: 2 * time.Second}
	start := time.Now()
	err := s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "api exited") {
		t.Fatalf("want api exit error, got %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("worker was not stopped: run took %v", d)
	}
}

func TestCancelTerminatesChildren(t *testing.T) {
	s := &Supervisor{Procs: []Proc{
		{Name: "worker-1", Path: sh(t), Args: []string{"-c", "sleep 30"}, Restart: true},
		{Name: "worker-2", Path: sh(t), Args: []string{"-c", "sleep 30"}, Restart: true},
	}, StopGrace: 2 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	if err := s.Run(ctx); err != nil {
		t.Fatalf("run after cancel: %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("children outlived the cancel by %v", d)
	}
	if s.Starts() != 2 {
		t.Fatalf("starts = %d, want 2 (no restarts during shutdown)", s.Starts())
	}
}
