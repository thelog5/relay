package jobs

import (
	"testing"
	"time"
)

func TestBackoffWithoutJitter(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 60 * time.Second}
	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		60 * time.Second, // capped
		60 * time.Second,
	}
	for i, w := range want {
		if got := b.Delay(i + 1); got != w {
			t.Errorf("attempt %d: got %v want %v", i+1, got, w)
		}
	}
}

func TestBackoffHandlesSillyInput(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 60 * time.Second}
	if got := b.Delay(0); got != time.Second {
		t.Fatalf("attempt 0 should behave like 1, got %v", got)
	}
	if got := b.Delay(1000); got != 60*time.Second {
		t.Fatalf("huge attempt should cap, got %v", got)
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	b := DefaultBackoff()
	for attempt := 1; attempt <= 10; attempt++ {
		floor := Backoff{Base: b.Base, Max: b.Max}.Delay(attempt)
		for i := 0; i < 200; i++ {
			got := b.Delay(attempt)
			if got < floor {
				t.Fatalf("attempt %d: %v below floor %v", attempt, got, floor)
			}
			ceil := floor + time.Duration(float64(floor)*b.JitterFrac)
			if ceil > b.Max {
				ceil = b.Max
			}
			if got > ceil {
				t.Fatalf("attempt %d: %v above ceiling %v", attempt, got, ceil)
			}
		}
	}
}

func TestBackoffJitterIsDeterministicWithInjectedRand(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 60 * time.Second, JitterFrac: 0.2, rand: func() float64 { return 0.5 }}
	if got, want := b.Delay(3), 4*time.Second+400*time.Millisecond; got != want {
		t.Fatalf("got %v want %v", got, want)
	}
}
