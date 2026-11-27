"""DLQ verification on the real primary pipeline.

* ``malformed`` - drives the demo control API's built-in ``malformed_event``
  scenario (real primary pipeline, real validation path) for each malformed
  case the API supports, and reads the resulting commerce.events.dlq records
  directly to verify metadata completeness.

Transient retry has no organic trigger in production traffic; it is
verified by the Go processor's tests instead (see
tests/integration/test_processor_reliability.py).
"""

import argparse
import sys
import time
from typing import Any

from scripts.benchmark.artifacts import now_iso, phase_path, write_json
from scripts.benchmark.config import BenchmarkConfig, derive_seed, load_config
from scripts.benchmark.demo_api import DemoApiClient
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

MALFORMED_CASES = [
    "malformed_json",
    "missing_field",
    "unknown_event_type",
    "payload_mismatch",
]


def run_malformed(
    config: BenchmarkConfig,
    api: DemoApiClient,
    *,
    event_count: int,
    events_per_second: int,
) -> dict[str, Any]:
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
                "dlq_records_from_postgres_time_window": int(db_dlq["rows"])
                if db_dlq
                else None,
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
            "(services/event_processor_go/internal/dlq) publishes exclusively to the "
            "commerce.events.dlq Kafka topic, which is the authoritative "
            "source used for dlq_records_from_kafka above."
        ),
        "captured_at": now_iso(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("sub_test", choices=["malformed"])
    parser.add_argument("--event-count", type=int, default=40)
    parser.add_argument("--events-per-second", type=int, default=20)
    args = parser.parse_args()

    config = load_config(args.run_tag)

    api = DemoApiClient(config.demo_api_base_url)
    result = run_malformed(
        config,
        api,
        event_count=args.event_count,
        events_per_second=args.events_per_second,
    )
    out_name = "retry_dlq_malformed"

    out_path = phase_path(config.phase_dir(), out_name)
    write_json(out_path, result)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
