#!/usr/bin/env bash
set -euo pipefail

readonly bootstrap_server="kafka:9092"
readonly max_attempts=30
readonly retry_interval_seconds=2
readonly kafka_topics="/opt/kafka/bin/kafka-topics.sh"

wait_for_kafka() {
  local attempt

  for ((attempt = 1; attempt <= max_attempts; attempt++)); do
    if "${kafka_topics}" --bootstrap-server "${bootstrap_server}" --list >/dev/null 2>&1; then
      echo "Kafka is ready."
      return 0
    fi

    echo "Waiting for Kafka (${attempt}/${max_attempts})..."
    sleep "${retry_interval_seconds}"
  done

  echo "Kafka did not become ready after ${max_attempts} attempts." >&2
  return 1
}

create_topic() {
  local topic="$1"
  local partitions="$2"

  "${kafka_topics}" \
    --bootstrap-server "${bootstrap_server}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config cleanup.policy=delete
}

verify_topic() {
  local topic="$1"

  if ! "${kafka_topics}" \
    --bootstrap-server "${bootstrap_server}" \
    --describe \
    --topic "${topic}" >/dev/null 2>&1; then
    echo "Required topic is missing: ${topic}" >&2
    return 1
  fi
}

wait_for_kafka
create_topic "commerce.events" 3
create_topic "commerce.events.dlq" 1
create_topic "commerce.fraud.alerts" 3

verify_topic "commerce.events"
verify_topic "commerce.events.dlq"
verify_topic "commerce.fraud.alerts"

echo "Final topic list:"
"${kafka_topics}" --bootstrap-server "${bootstrap_server}" --list
echo "Kafka topic initialization completed successfully."
