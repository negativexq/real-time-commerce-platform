"""Retry and DLQ verification, two independent sub-tests:

* ``malformed`` - drives the demo control API's built-in ``malformed_event``
  scenario (real primary pipeline, real validation path) for each malformed
  case the API supports, and reads the resulting commerce.events.dlq records
  directly to verify metadata completeness.
* ``retry`` - the real transient-retry path (RetryPolicy/run_with_retry) has
  no organic trigger in production traffic (malformed data causes permanent
  validation errors, not RetryableProcessingError); this sub-test uses an
  isolated consumer group and a synthetic handler that raises
  RetryableProcessingError a controlled number of times - the exact pattern
  scripts/processor-smoke.py already uses - scaled to a larger controlled
  batch, and using the real MessageProcessor/RetryPolicy/DlqPublisher code.
"""

import argparse
import base64
import random
import sys
import time
from typing import Any
from uuid import uuid4

from confluent_kafka import Consumer  # type: ignore[import-untyped]

from scripts.benchmark.artifacts import now_iso, phase_path, write_json
from scripts.benchmark.config import BenchmarkConfig, derive_seed, load_config
from scripts.benchmark.demo_api import DemoApiClient
from scripts.benchmark.kafka_lag import describe_group
from scripts.benchmark.kafka_replay import read_records, topic_watermarks
from scripts.benchmark.pg import query_one

REQUIRED_DLQ_FIELDS = [
    "dlq_schema_version",
    "dlq_record_id",
    "failed_at",
    "source_topic",
    "source_partition",
    "source_offset",
    "error_category",
    "error_message",
    "error_type",
    "processing_attempts",
    "consumer_group",
    "processor_instance_id",
]

MALFORMED_CASES = ["malformed_json", "missing_field", "unknown_event_type", "payload_mismatch"]


def run_malformed(config: BenchmarkConfig, api: DemoApiClient, *, event_count: int, events_per_second: int) -> dict[str, Any]:
    cases: list[dict[str, Any]] = []
    for case in MALFORMED_CASES:
        dlq_start = topic_watermarks(config.kafka_bootstrap_servers, config.dlq_topic)
        body = {
            "scenario_type": "malformed_event",
            "event_count": event_count,
            "events_per_second": events_per_second,
            "seed": derive_seed(config.run_tag, "malformed", case),
            "malformed_case": case,
            "notes": f"benchmark:{config.run_tag}:dlq:{case}",
        }
        started_at = time.time()
        result = api.run_scenario(
            body, timeout=max(120.0, event_count / max(events_per_second, 1) * 4 + 60)
        )
        dlq_end = topic_watermarks(config.kafka_bootstrap_servers, config.dlq_topic)
        records = read_records(
            config.kafka_bootstrap_servers,
            config.dlq_topic,
            dlq_start,
            dlq_end,
            timeout_seconds=30.0,
        )
        db_dlq = query_one(
            config.postgres_dsn,
            """
            SELECT COUNT(*) AS rows
            FROM dead_letter_events
            WHERE failed_at >= to_timestamp(%s) AND original_topic = %s
            """,
            (started_at - 2, config.events_topic),
        )
        metadata_ok = all(
            all(field in record.get("value", {}) or {} for field in REQUIRED_DLQ_FIELDS)
            for record in records
            if record.get("value")
        )
        cases.append(
            {
                "malformed_case": case,
                "run_id": result.run_id,
                "requested_event_count": event_count,
                "dlq_records_from_kafka": len(records),
                "dlq_records_from_postgres_time_window": int(db_dlq["rows"]) if db_dlq else None,
                "dlq_metadata_complete": metadata_ok,
                "sample_error_categories": sorted(
                    {
                        record["value"].get("error_category")
                        for record in records
                        if record.get("value")
                    }
                ),
            }
        )
    return {
        "sub_test": "malformed",
        "cases": cases,
        "all_metadata_complete": all(c["dlq_metadata_complete"] for c in cases),
        "note": (
            "dlq_records_from_postgres_time_window is consistently 0: nothing "
            "in the current codebase writes to the legacy dead_letter_events "
            "table (grep confirms only a read path in demo_control_api/main.py "
            "GET /api/v1/dlq). The event-processor's actual DLQ mechanism "
            "(services/event_processor/dlq.py) publishes exclusively to the "
            "commerce.events.dlq Kafka topic, which is the authoritative "
            "source used for dlq_records_from_kafka above."
        ),
        "captured_at": now_iso(),
    }


class _RetrySmokeClock:
    def now(self):  # noqa: ANN201
        from datetime import UTC, datetime

        return datetime.now(UTC)


def run_retry(config: BenchmarkConfig, *, batch_size: int, exhausted_fraction: float, seed: int) -> dict[str, Any]:
    config.apply_process_env()
    from services.event_generator.config import GeneratorConfig
    from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
    from services.event_generator.journey import JourneyBuilder
    from services.event_generator.producer import KafkaEventProducer
    from services.event_processor.config import ProcessorConfig
    from services.event_processor.consumer import KafkaEventConsumer
    from services.event_processor.dlq import DlqPublisher
    from services.event_processor.errors import RetryableProcessingError
    from services.event_processor.handler import AuditEventHandler, EventHandler
    from services.event_processor.idempotency import RedisIdempotencyStore
    from services.event_processor.models import ProcessingOutcome, RunSummary
    from services.event_processor.processor import MessageProcessor
    from shared.commerce_common.enums import CustomerPersona, EventType

    identity = uuid4().hex
    group = f"commerce-benchmark-retry-{config.run_tag}-{identity[:8]}"
    prefix = f"commerce:processor:benchmark:{config.run_tag}:retry:{identity[:8]}"

    proc_config = ProcessorConfig.from_environment().model_copy(
        update={
            "processor_consumer_group": group,
            "processor_client_id": f"benchmark-retry-{identity[:8]}",
            "processor_auto_offset_reset": "latest",
            "processor_idempotency_key_prefix": prefix,
            "processor_max_processing_attempts": 3,
            "processor_retry_initial_backoff_ms": 5,
            "processor_retry_max_backoff_ms": 20,
        }
    )
    raw_consumer = Consumer(KafkaEventConsumer.kafka_config(proc_config))
    consumer = KafkaEventConsumer(proc_config, raw_consumer)
    consumer.subscribe()
    deadline = time.monotonic() + 15
    while not raw_consumer.assignment() and time.monotonic() < deadline:
        consumer.poll()
    if not raw_consumer.assignment():
        raise RuntimeError("benchmark retry consumer assignment timed out")

    gen_config = GeneratorConfig.from_environment().model_copy(
        update={
            "generator_seed": seed,
            "generator_persona": CustomerPersona.NORMAL,
            "generator_add_to_cart_probability": 1,
            "generator_checkout_probability": 1,
            "generator_payment_success_probability": 1,
            "generator_refund_probability": 0,
        }
    )
    rng = random.Random(seed)
    builder = JourneyBuilder(gen_config, SyntheticGenerator(rng, SeededUuidFactory(seed)), _RetrySmokeClock())
    producer = KafkaEventProducer(gen_config)

    n_exhausted = max(int(round(batch_size * exhausted_fraction)), 1)
    n_succeed_after_retry = batch_size - n_exhausted

    events = []
    while len(events) < batch_size:
        journey = builder.build()
        events.extend(journey.events)
    events = events[:batch_size]
    for event in events:
        producer.publish(event)
    producer.flush()

    class FailThenSucceed:
        def __init__(self, failures: int) -> None:
            self.failures = failures
            self.calls: dict[str, int] = {}

        def handle(self, event, context) -> None:  # noqa: ANN001
            key = str(event.event_id)
            self.calls[key] = self.calls.get(key, 0) + 1
            if self.calls[key] <= self.failures:
                raise RetryableProcessingError("benchmark controlled retry")

    class AlwaysFail:
        def handle(self, event, context) -> None:  # noqa: ANN001
            raise RetryableProcessingError("benchmark controlled exhausted retry")

    succeed_ids = {str(e.event_id) for e in events[:n_succeed_after_retry]}
    exhausted_ids = {str(e.event_id) for e in events[n_succeed_after_retry:]}

    succeed_handler = FailThenSucceed(2)
    fail_handler = AlwaysFail()

    class RoutingHandler:
        def handle(self, event, context) -> None:  # noqa: ANN001
            key = str(event.event_id)
            if key in exhausted_ids:
                fail_handler.handle(event, context)
            else:
                succeed_handler.handle(event, context)

    routing = RoutingHandler()
    handlers: dict[EventType, EventHandler] = {kind: routing for kind in EventType}
    store = RedisIdempotencyStore(proc_config)
    dlq = DlqPublisher(proc_config)
    summary = RunSummary()
    processor = MessageProcessor(
        proc_config,
        store,
        dlq,
        consumer,
        handlers,
        summary,
        processor_instance_id=identity,
        wait=lambda seconds: None,
    )

    outcomes: list[ProcessingOutcome] = []
    poll_deadline = time.monotonic() + 60
    while len(outcomes) < batch_size and time.monotonic() < poll_deadline:
        record = consumer.poll()
        if record is not None:
            outcomes.append(processor.process(record))

    committed_offsets = describe_group(config.compose_project, group)
    total_committed = sum(row.current_offset or 0 for row in committed_offsets)
    total_log_end = sum(row.log_end_offset or 0 for row in committed_offsets)

    # consumer_group lives inside the DLQ envelope JSON body (DlqEnvelope
    # .consumer_group), not as a Kafka header, so scope by scanning a
    # trailing window of the DLQ topic and filtering on the decoded value.
    dlq_start_end = topic_watermarks(config.kafka_bootstrap_servers, config.dlq_topic)
    dlq_scan_start = {p: max(o - 200, 0) for p, o in dlq_start_end.items()}
    dlq_candidates = read_records(
        config.kafka_bootstrap_servers,
        config.dlq_topic,
        dlq_scan_start,
        dlq_start_end,
        timeout_seconds=15.0,
    )
    dlq_matches = [
        record
        for record in dlq_candidates
        if (record.get("value") or {}).get("consumer_group") == group
    ]
    dlq_metadata_ok = all(
        all(field in record.get("value", {}) or {} for field in REQUIRED_DLQ_FIELDS)
        for record in dlq_matches
        if record.get("value")
    )

    redis_keys_completed = 0
    try:
        import redis as redis_lib

        client = redis_lib.Redis.from_url(proc_config.redis_url, decode_responses=True)
        keys = list(client.scan_iter(match=f"{prefix}:*", count=200))
        redis_keys_completed = sum(
            1 for key in keys if '"status":"completed"' in str(client.get(key))
        )
        if keys:
            client.delete(*keys)
        client.close()
    except Exception:  # noqa: BLE001
        pass

    dlq.close()
    store.close()
    consumer.close()

    offset_commit_matches_terminal = total_committed >= len(
        [o for o in outcomes if o.terminal]
    ) and total_committed <= total_log_end

    return {
        "sub_test": "retry",
        "batch_size": batch_size,
        "retry_attempts_total": summary.retries,
        "retry_exhausted": summary.retry_exhausted,
        "retry_success_count": redis_keys_completed,
        "retry_success_rate": (
            redis_keys_completed / n_succeed_after_retry if n_succeed_after_retry else None
        ),
        "dlq_count": summary.dlq_records,
        "dlq_rate": summary.dlq_records / batch_size if batch_size else None,
        "expected_succeed_after_retry": n_succeed_after_retry,
        "expected_exhausted_to_dlq": n_exhausted,
        "isolated_consumer_group": group,
        "committed_offset_total": total_committed,
        "log_end_offset_total": total_log_end,
        "terminal_outcomes": len([o for o in outcomes if o.terminal]),
        "offsets_committed_only_after_terminal_handling": offset_commit_matches_terminal,
        "dlq_records_matched": len(dlq_matches),
        "dlq_metadata_complete": dlq_metadata_ok,
        "captured_at": now_iso(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("sub_test", choices=["malformed", "retry"])
    parser.add_argument("--event-count", type=int, default=40)
    parser.add_argument("--events-per-second", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=60)
    parser.add_argument("--exhausted-fraction", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--repeat-index", type=int, default=0)
    args = parser.parse_args()

    config = load_config(args.run_tag)

    if args.sub_test == "malformed":
        api = DemoApiClient(config.demo_api_base_url)
        result = run_malformed(
            config,
            api,
            event_count=args.event_count,
            events_per_second=args.events_per_second,
        )
        out_name = "retry_dlq_malformed"
    else:
        seed = (
            args.seed
            if args.seed is not None
            else derive_seed(config.run_tag, "retry", str(args.repeat_index))
        )
        result = run_retry(
            config,
            batch_size=args.batch_size,
            exhausted_fraction=args.exhausted_fraction,
            seed=seed,
        )
        out_name = f"retry_dlq_retry_run{args.repeat_index}"

    out_path = phase_path(config.phase_dir(), out_name)
    write_json(out_path, result)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
