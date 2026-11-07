// Package testutil provisions throwaway Postgres databases and Redis key
// prefixes for integration tests. Tests are skipped unless the
// RELAY_TEST_POSTGRES_URL / RELAY_TEST_REDIS_URL variables are set, so plain
// `go test ./...` works on a laptop with nothing running.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// RandomSuffix returns a short random hex string.
func RandomSuffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// PostgresDSN creates a fresh database for this test and returns its DSN.
// The database is dropped when the test ends.
func PostgresDSN(t testing.TB) string {
	t.Helper()
	base := os.Getenv("RELAY_TEST_POSTGRES_URL")
	if base == "" {
		t.Skip("RELAY_TEST_POSTGRES_URL not set; skipping Postgres integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect admin postgres: %v", err)
	}
	defer admin.Close(ctx)

	name := "relay_test_" + RandomSuffix()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := pgx.Connect(ctx, base)
		if err != nil {
			return
		}
		defer c.Close(ctx)
		_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse RELAY_TEST_POSTGRES_URL: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

// RedisURL returns the test Redis URL and a unique key prefix so parallel
// test packages never see each other's streams.
func RedisURL(t testing.TB) (string, string) {
	t.Helper()
	u := os.Getenv("RELAY_TEST_REDIS_URL")
	if u == "" {
		t.Skip("RELAY_TEST_REDIS_URL not set; skipping Redis integration test")
	}
	return u, fmt.Sprintf("relaytest-%s", RandomSuffix())
}
