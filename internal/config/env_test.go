package config

import (
	"testing"
	"time"
)

func TestEnvHelpers(t *testing.T) {
	t.Setenv("RELAY_T_STR", "hello")
	t.Setenv("RELAY_T_INT", "42")
	t.Setenv("RELAY_T_DUR", "1500ms")
	t.Setenv("RELAY_T_BOOL", "true")
	if String("RELAY_T_STR", "x") != "hello" || String("RELAY_T_MISSING", "x") != "x" {
		t.Fatal("String")
	}
	if Int("RELAY_T_INT", 1) != 42 || Int("RELAY_T_MISSING", 1) != 1 {
		t.Fatal("Int")
	}
	if Duration("RELAY_T_DUR", 0) != 1500*time.Millisecond || Duration("RELAY_T_MISSING", time.Second) != time.Second {
		t.Fatal("Duration")
	}
	if !Bool("RELAY_T_BOOL", false) || Bool("RELAY_T_MISSING", false) {
		t.Fatal("Bool")
	}
}

func TestIntPanicsOnGarbage(t *testing.T) {
	t.Setenv("RELAY_T_BAD", "lots")
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	Int("RELAY_T_BAD", 1)
}
