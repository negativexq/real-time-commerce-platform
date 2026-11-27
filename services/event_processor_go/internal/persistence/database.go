// Package persistence ports services.event_processor.persistence: one
// explicit PostgreSQL transaction per source event that atomically commits
// the processed-event ledger, business effects, fraud evaluation and outbox.
package persistence

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

// SafeEndpoint returns a credential-free endpoint for diagnostics.
func SafeEndpoint(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "unknown"
	}
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	return fmt.Sprintf("%s://%s:%s%s", u.Scheme, u.Hostname(), port, u.Path)
}

type Database struct {
	cfg        *config.Processor
	poolConfig *pgxpool.Config
	Pool       *pgxpool.Pool
}

func NewDatabase(cfg *config.Processor) (*Database, error) {
	pc, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		return nil, procerr.Wrap(procerr.StartupDatabase, "invalid POSTGRES_DSN", err)
	}
	pc.MinConns = int32(cfg.DBPoolMinSize)
	pc.MaxConns = int32(cfg.DBPoolMaxSize)
	pc.MaxConnIdleTime = cfg.DBMaxIdle
	pc.HealthCheckPeriod = cfg.DBHealthcheckInterval
	pc.ConnConfig.ConnectTimeout = time.Duration(cfg.DBConnectTimeoutSeconds) * time.Second
	pc.ConnConfig.RuntimeParams["statement_timeout"] = strconv.Itoa(cfg.DBStatementTimeoutMs)
	return &Database{cfg: cfg, poolConfig: pc}, nil
}

// Open creates the pool and runs bounded health and schema checks.
func (d *Database) Open(ctx context.Context) error {
	pool, err := pgxpool.NewWithConfig(ctx, d.poolConfig)
	if err != nil {
		return procerr.Wrap(procerr.StartupDatabase, "PostgreSQL pool creation failed", err)
	}
	d.Pool = pool
	var last error
	for attempt := 1; attempt <= d.cfg.DBStartupAttempts; attempt++ {
		if last = d.check(ctx); last == nil {
			return nil
		}
		slog.Warn("database_startup_retry", "attempt", attempt,
			"endpoint", SafeEndpoint(d.cfg.PostgresDSN), "error_type", fmt.Sprintf("%T", last))
		if attempt < d.cfg.DBStartupAttempts {
			time.Sleep(d.cfg.DBStartupBackoff)
		}
	}
	d.Close()
	return procerr.Wrap(procerr.StartupDatabase, "PostgreSQL startup checks failed", last)
}

func (d *Database) check(ctx context.Context) error {
	actx, cancel := context.WithTimeout(ctx, d.cfg.DBAcquireTimeout)
	defer cancel()
	var one int
	if err := d.Pool.QueryRow(actx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		return procerr.Wrap(procerr.RetryableDatabase, "PostgreSQL is unavailable", err)
	}
	var version int
	if err := d.Pool.QueryRow(actx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&version); err != nil {
		return procerr.Wrap(procerr.StartupDatabase, "database migration history is unavailable", err)
	}
	if version < d.cfg.RequiredSchemaVersion {
		return procerr.New(procerr.StartupDatabase,
			fmt.Sprintf("database schema version %d is behind required %d", version, d.cfg.RequiredSchemaVersion))
	}
	return nil
}

func (d *Database) Close() {
	if d.Pool != nil {
		d.Pool.Close()
	}
}
