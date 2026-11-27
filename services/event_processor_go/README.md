# event_processor_go

The event processor that runs in Compose: both `event-processor` and
`demo-verification-processor` build from this directory. It is a Go port of
the retired Python processor (`services/event_processor`, removed on
2026-11-27; see git history) and kept its external contract:

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

The binary has these subcommands besides the processor itself:

- `dlq-sink` copies `commerce.events.dlq` into the PostgreSQL `dead_letters`
  table (the `dlq-sink` Compose service, port 9104). See
  [docs/event-processor.md](../../docs/event-processor.md#dlq-sink).
- `healthcheck [file]` exits 0 if the health file exists. It defaults to
  `/tmp/event-processor-healthy`; the sink uses `/tmp/dlq-sink-healthy`.
- `fraud-rules` and `fraud-config` are the fraud diagnostics (`make fraud-rules`, `make fraud-config-check`).

The Python processor has been removed. Database migrations moved to
`services/db_migrations` (the `postgres-migrate` service), the outbox
publisher reads its own configuration, and the fraud diagnostics are
subcommands of this binary (`make fraud-rules`, `make fraud-config-check`).

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

### Frozen Python references

Before the Python processor was deleted, its behaviour was recorded as test
fixtures. They can no longer be regenerated, so treat them as the contract:

- `internal/fraud/testdata/python_reference.json`: 600 seeded fraud contexts
  around every rule boundary under four configurations, with the exact
  rule-results JSON, score, decision, severity, evaluation ID and alert
  message (payload bytes, key, headers). Checked by
  `TestEvaluationsMatchPythonReference`.
- `internal/dlq/testdata/validation_reference.json`: 33 Kafka records
  covering every validation category, with the error hints and the exact
  DLQ record bytes, key and headers. Checked by
  `TestValidationAndDLQMatchPythonReference`.

Change a fixture only for an intentional contract change, and say so in the
commit.

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

The Redis lease scripts (`internal/idempotency`) and the persistence layer
(`internal/persistence`, every handler plus the fraud context queries, run
against a freshly migrated database per test) are tested against real
services. They skip unless `PROCESSOR_TEST_REDIS_URL` and
`PROCESSOR_TEST_POSTGRES_DSN` are set; `make go-service-test` sets them for
the local Compose stack, and CI runs them with
`PROCESSOR_TEST_REQUIRE_SERVICES=1`, so a missing service fails the build.

`make processor-smoke` builds the image and runs
`tests/integration/test_processor_reliability.py`, which drives this
container in finite mode against the Compose stack.

The shared event contracts are still Python (`shared/schemas`). To regenerate
the contract golden fixtures after a contract change, run this from the
repository root:

```bash
.venv/bin/python services/event_processor_go/internal/contracts/testdata/generate_golden.py
```
