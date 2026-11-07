// Package config reads settings from environment variables with defaults, so
// every service is configured the same way under docker compose.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// String returns $key or def.
func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// Int returns $key parsed as an int, or def. Panics on garbage so a typo in
// docker-compose.yml fails loudly at startup.
func Int(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s=%q is not an integer", key, v))
	}
	return n
}

// Duration returns $key parsed with time.ParseDuration, or def.
func Duration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s=%q is not a duration (e.g. 5s, 500ms)", key, v))
	}
	return d
}

// Bool returns $key parsed with strconv.ParseBool, or def.
func Bool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s=%q is not a boolean", key, v))
	}
	return b
}
