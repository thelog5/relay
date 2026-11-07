package jobs

import "testing"

func TestLifecycleTransitions(t *testing.T) {
	allowed := []struct{ from, to Status }{
		{"", StatusQueued},
		{StatusQueued, StatusRunning},
		{StatusRunning, StatusSucceeded},
		{StatusRunning, StatusRetryScheduled},
		{StatusRetryScheduled, StatusQueued},
		{StatusRunning, StatusDead},
		{StatusRunning, StatusRunning}, // reclaimed from a crashed worker
		{StatusDead, StatusQueued},     // manual retry from the dashboard
		{StatusQueued, StatusDead},     // enqueue to Redis failed
	}
	for _, tc := range allowed {
		if !CanTransition(tc.from, tc.to) {
			t.Errorf("expected %q -> %q to be allowed", tc.from, tc.to)
		}
	}

	forbidden := []struct{ from, to Status }{
		{"", StatusRunning},
		{StatusQueued, StatusSucceeded},
		{StatusSucceeded, StatusQueued},
		{StatusSucceeded, StatusRunning},
		{StatusRetryScheduled, StatusRunning},
		{StatusDead, StatusRunning},
	}
	for _, tc := range forbidden {
		if CanTransition(tc.from, tc.to) {
			t.Errorf("expected %q -> %q to be rejected", tc.from, tc.to)
		}
	}
}

func TestParseStatus(t *testing.T) {
	for _, s := range AllStatuses {
		got, err := ParseStatus(string(s))
		if err != nil || got != s {
			t.Fatalf("ParseStatus(%q) = %q, %v", s, got, err)
		}
	}
	if _, err := ParseStatus("exploded"); err == nil {
		t.Fatal("expected error for unknown status")
	}
}

func TestTerminal(t *testing.T) {
	if !StatusSucceeded.Terminal() || !StatusDead.Terminal() {
		t.Fatal("succeeded and dead must be terminal")
	}
	if StatusQueued.Terminal() || StatusRunning.Terminal() || StatusRetryScheduled.Terminal() {
		t.Fatal("non-final statuses reported terminal")
	}
}
