# event_processor_go

The event processor that runs in Compose: both `event-processor` and
`demo-verification-processor` build from this directory. It is a Go port of
`services/event_processor` and a drop-in replacement for it:

- the same environment variables and CLI flags;
- the same Kafka topics and consumer-group semantics;
- the same Redis keys and Lua scripts;
- the same PostgreSQL SQL;
- the same metric names and buckets, `/health` endpoint, port 9101 and
  `/tmp/event-processor-healthy` file.

Go replaced Python on 2026-11-27 after a benchmark A/B
([`artifacts/benchmark/runtime-ab-20261127/`](../../artifacts/benchmark/runtime-ab-20261127/README.md)).
At the same load, Go had 4–19x lower end-to-end p95 latency and 30–35% less
processor CPU. Its sustainable ceiling was just above 1400 evt/s, against
1200–1400 evt/s for Python.

```bash
make processor-up        # build and start the Go processor
make go-check            # gofmt, go vet, go test
```

The Python processor stays in the repository for now. It still provides
database migrations (`postgres-migrate`), the fraud configuration that
`fraud_outbox_publisher` imports, and the in-process smoke scripts, which run
in the `processor-tools` service.

## Parity guarantees

- **Canonical JSON and `payload_sha256`** match Python's
  `json.dumps(sort_keys=True, ensure_ascii=False, separators=(",", ":"))`
  byte for byte. This includes Python `Decimal.__str__` and the UTC
  timestamp forms. The proof is 55 golden cases generated from the Python
  code: `internal/contracts/testdata/generate_golden.py` produces them and
  `go test ./internal/contracts` checks them.
- **Validation** checks rules in the same order and emits the same
  categories. It follows pydantic strict-mode rules: extra fields are
  forbidden and nothing is coerced.
- **Deterministic IDs** use the same uuid5 namespaces. This covers DLQ
  records, fraud evaluations, alerts, alert events and outbox rows.
- **Processing flow** follows `MessageProcessor.process` step for step:
  1. Validate.
  2. Reserve a Redis lease.
  3. Persist with bounded, jittered retry.
  4. Complete the Redis lease.
  5. Commit the offset in a batch, never past a gap.

  DLQ categories and attempt counts are the same as in Python.
- **Fraud engine**: the context queries, all 10 rules, aggregation and
  thresholds are ported, with the same evidence keys.

### Differential check

The check ran on 2026-11-27. The same Kafka topic held 2,085 records: five
personas plus the full anomaly mix. Each processor consumed it into its own
fresh database. Results:

- Every table had the same row count.
- Every column was identical except those set by the database clock
  (`created_at`, `updated_at`, `processed_at`, `available_at`) and
  `processor_instance_id`, which comes from the hostname.
- All 132 DLQ records had the same ID, key, headers, category and error
  message.
- The run summaries matched counter for counter.

## Deliberate deviations

- **Transport errors are retried.** A PostgreSQL failure without a SQLSTATE
  is classified as `RetryableDatabaseError` instead of being dead-lettered.
  Examples are a connection reset, a broken pipe or a timeout.
- **pgx runs in `cache_describe` mode.** Queries go out as unnamed
  statements, so PostgreSQL plans each execution against current
  statistics. pgx's default mode creates a named prepared statement per
  query instead. PostgreSQL can then pin a generic plan after five
  executions; if that happens while tables are nearly empty, the plan stays
  a sequential scan until the next ANALYZE. That happened in the first
  repeat of the 2026-11-27 benchmark A/B.
- **Redis scripts run with EVALSHA** through `redis.NewScript`. The script
  text is the same as in Python.
- **SQL-statement classification for metrics is memoized**, since the SQL
  text is static.
- **Only `PROCESSOR_WORKER_POOL_SIZE=1` is supported**, which is the default.
  The reverted pooled mode was not ported.
- **Kafka errors:** fatal errors and an unknown topic stop the loop. Other
  `kafka.Error` events are logged and polling continues, the same as the
  Python `poll()` handling.

## Development

```bash
cd services/event_processor_go
go build ./... && go vet ./... && go test ./...
```

To regenerate the golden fixtures after a contract change in the Python
code, run this from the repository root:

```bash
.venv/bin/python services/event_processor_go/internal/contracts/testdata/generate_golden.py
```
