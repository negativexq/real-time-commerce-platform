#!/usr/bin/env bash
set -euo pipefail

readonly bootstrap_server="kafka:9092"
readonly kafka_topics="/opt/kafka/bin/kafka-topics.sh"
readonly kafka_producer="/opt/kafka/bin/kafka-console-producer.sh"
readonly kafka_consumer="/opt/kafka/bin/kafka-console-consumer.sh"
readonly required_topics=(
  "commerce.events"
  "commerce.events.dlq"
  "commerce.fraud.alerts"
)

if ! "${kafka_topics}" --bootstrap-server "${bootstrap_server}" --list >/dev/null; then
  echo "Kafka is not healthy." >&2
  exit 1
fi

topic_list="$("${kafka_topics}" --bootstrap-server "${bootstrap_server}" --list)"
for topic in "${required_topics[@]}"; do
  if ! grep -Fxq "${topic}" <<<"${topic_list}"; then
    echo "Required topic is missing: ${topic}" >&2
    exit 1
  fi
done

event_id="smoke-$(date -u +%Y%m%dT%H%M%SZ)-$$"
timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
event="{\"event_id\":\"${event_id}\",\"event_type\":\"smoke_test\",\"event_version\":1,\"event_time\":\"${timestamp}\",\"produced_at\":\"${timestamp}\",\"source\":\"kafka-smoke\",\"correlation_id\":\"${event_id}\",\"payload\":{\"message\":\"Kafka smoke test\"}}"

printf '%s\n' "${event}" | "${kafka_producer}" \
  --bootstrap-server "${bootstrap_server}" \
  --topic commerce.events

consumed="$("${kafka_consumer}" \
  --bootstrap-server "${bootstrap_server}" \
  --topic commerce.events \
  --from-beginning \
  --timeout-ms 10000 2>/dev/null || true)"

if ! grep -Fq "\"event_id\":\"${event_id}\"" <<<"${consumed}"; then
  echo "Smoke-test event was not consumed before the timeout." >&2
  exit 1
fi

echo "Kafka smoke test passed for event ${event_id}."
