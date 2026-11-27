// Package testenv gives tests access to real Redis and PostgreSQL.
//
// Tests that need them read PROCESSOR_TEST_REDIS_URL and
// PROCESSOR_TEST_POSTGRES_DSN. When a variable is unset the test is skipped,
// unless PROCESSOR_TEST_REQUIRE_SERVICES=1 (set in CI), which turns the skip
// into a failure so missing services can never pass silently.
//
//	make go-service-test   # runs them against the local Compose stack
package testenv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func unavailable(t testing.TB, format string, args ...any) {
	t.Helper()
	if os.Getenv("PROCESSOR_TEST_REQUIRE_SERVICES") == "1" {
		t.Fatalf(format, args...)
	}
	t.Skipf(format, args...)
}

// RedisURL returns the test Redis URL or skips the test.
func RedisURL(t testing.TB) string {
	t.Helper()
	v := os.Getenv("PROCESSOR_TEST_REDIS_URL")
	if v == "" {
		unavailable(t, "PROCESSOR_TEST_REDIS_URL is not set")
	}
	return v
}

// UniqueSuffix is a short random hex string for keys and database names.
func UniqueSuffix() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func migrationsDir() string {
	_, file, _, _ := runtime.Caller(0)
	// internal/testenv -> repository root
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "database", "migrations")
}

// FreshDatabase creates an empty database with every migration applied and
// drops it when the test ends. It returns the new database's DSN.
func FreshDatabase(t testing.TB) string {
	t.Helper()
	admin := os.Getenv("PROCESSOR_TEST_POSTGRES_DSN")
	if admin == "" {
		unavailable(t, "PROCESSOR_TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		unavailable(t, "PostgreSQL is unreachable: %v", err)
	}
	defer conn.Close(ctx)
	name := "processor_test_" + UniqueSuffix()
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := pgx.Connect(ctx, admin)
		if err != nil {
			t.Logf("drop %s: %v", name, err)
			return
		}
		defer c.Close(ctx)
		if _, err := c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Logf("drop %s: %v", name, err)
		}
	})

	u, err := url.Parse(admin)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	dsn := u.String()
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	files, err := filepath.Glob(filepath.Join(migrationsDir(), "*.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no migrations found in %s: %v", migrationsDir(), err)
	}
	sort.Strings(files)
	if _, err := db.Exec(ctx, `CREATE TABLE schema_migrations (
		version INTEGER PRIMARY KEY, name TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for i, file := range files {
		sql, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		// pgx sends argument-free Exec over the simple protocol, so a file
		// with many statements runs as one implicit transaction.
		if _, err := db.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", filepath.Base(file), err)
		}
		if _, err := db.Exec(ctx, "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)",
			i+1, filepath.Base(file)); err != nil {
			t.Fatal(err)
		}
	}
	return dsn
}
