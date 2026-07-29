#!/usr/bin/env bash
set -euo pipefail

readonly required_tables=(
  "dead_letter_events"
  "fraud_alerts"
  "processed_events"
)

postgres_exec() {
  docker compose exec -T postgres sh -c \
    'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"' \
    -- "$@"
}

if ! docker compose exec -T postgres sh -c \
  'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' >/dev/null; then
  echo "PostgreSQL is not healthy." >&2
  exit 1
fi

if [[ "$(docker compose exec -T redis redis-cli ping | tr -d '\r')" != "PONG" ]]; then
  echo "Redis is not healthy." >&2
  exit 1
fi

actual_tables="$(
  postgres_exec -Atqc \
    "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;"
)"
for table in "${required_tables[@]}"; do
  if ! grep -Fxq "${table}" <<<"${actual_tables}"; then
    echo "Required PostgreSQL table was not found: ${table}" >&2
    exit 1
  fi
done

event_id="$(postgres_exec -Atqc "SELECT gen_random_uuid();")"
correlation_id="$(postgres_exec -Atqc "SELECT gen_random_uuid();")"
redis_key="storage-smoke:${event_id}"

cleanup() {
  postgres_exec -qc \
    "DELETE FROM processed_events WHERE event_id = '${event_id}'::uuid;" \
    >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "${redis_key}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

postgres_exec \
  -qc "
    INSERT INTO processed_events (
      event_id,
      event_type,
      event_version,
      event_time,
      produced_at,
      source,
      correlation_id,
      payload_json
    ) VALUES (
      '${event_id}'::uuid,
      'storage_smoke_test',
      1,
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP,
      'storage-smoke',
      '${correlation_id}'::uuid,
      '{\"temporary\": true}'::jsonb
    );
  "

read_event_id="$(
  postgres_exec -Atqc \
    "SELECT event_id FROM processed_events WHERE event_id = '${event_id}'::uuid;"
)"
if [[ "${read_event_id}" != "${event_id}" ]]; then
  echo "Failed to read the PostgreSQL smoke-test row." >&2
  exit 1
fi

postgres_exec -qc \
  "DELETE FROM processed_events WHERE event_id = '${event_id}'::uuid;"

if [[ "$(postgres_exec -Atqc \
  "SELECT COUNT(*) FROM processed_events WHERE event_id = '${event_id}'::uuid;")" != "0" ]]; then
  echo "Failed to delete the PostgreSQL smoke-test row." >&2
  exit 1
fi

if [[ "$(docker compose exec -T redis redis-cli SET "${redis_key}" \
  "temporary" EX 60 | tr -d '\r')" != "OK" ]]; then
  echo "Failed to set the Redis smoke-test key." >&2
  exit 1
fi

if [[ "$(docker compose exec -T redis redis-cli GET "${redis_key}" \
  | tr -d '\r')" != "temporary" ]]; then
  echo "Failed to read the Redis smoke-test key." >&2
  exit 1
fi

if [[ "$(docker compose exec -T redis redis-cli DEL "${redis_key}" \
  | tr -d '\r')" != "1" ]]; then
  echo "Failed to delete the Redis smoke-test key." >&2
  exit 1
fi

trap - EXIT
echo "Storage smoke test passed for PostgreSQL event ${event_id} and Redis."
