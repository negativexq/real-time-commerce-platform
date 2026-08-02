"""Controlled duplicate-delivery / idempotency verification.

Drives the demo control API's built-in ``duplicate_delivery`` scenario
(services/demo_control_api/services/scenario_runner.py sets
generator_duplicate_event_probability=1 for this scenario type: the real
AnomalyInjector.prepare() tags one duplicate message per customer journey,
re-sending the same event_id) through the real primary pipeline, then
verifies durable uniqueness directly in Postgres.
"""

import argparse
import sys
from typing import Any

from scripts.benchmark.artifacts import now_iso, phase_path, write_json
from scripts.benchmark.config import BenchmarkConfig, derive_seed, load_config
from scripts.benchmark.demo_api import DemoApiClient
from scripts.benchmark.pg import query_all, query_one


def run_once(
    config: BenchmarkConfig,
    api: DemoApiClient,
    *,
    event_count: int,
    events_per_second: int,
    seed: int,
) -> dict[str, Any]:
    body = {
        "scenario_type": "duplicate_delivery",
        "event_count": event_count,
        "events_per_second": events_per_second,
        "seed": seed,
        "notes": f"benchmark:{config.run_tag}:idempotency",
    }
    result = api.run_scenario(
        body, timeout=max(120.0, event_count / max(events_per_second, 1) * 4 + 60)
    )
    run = result.summary.get("run", {})

    total_deliveries = int(run.get("generated_event_count", 0))
    duplicate_count_reported = int(run.get("duplicate_count", 0))

    manifest_stats = query_one(
        config.postgres_dsn,
        """
        SELECT COUNT(*) AS unique_event_ids
        FROM demo_run_event_manifest WHERE run_id = %s
        """,
        (result.run_id,),
    )
    unique_event_ids = int(manifest_stats["unique_event_ids"]) if manifest_stats else 0

    dedup_check = query_one(
        config.postgres_dsn,
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT pe.event_id) AS distinct_event_ids,
            COUNT(DISTINCT (pe.kafka_topic, pe.kafka_partition, pe.kafka_offset))
                AS distinct_kafka_sources
        FROM demo_run_event_manifest m
        JOIN processed_events pe ON pe.event_id = m.event_id
        WHERE m.run_id = %s
        """,
        (result.run_id,),
    )
    total_rows = int(dedup_check["total_rows"]) if dedup_check else 0
    distinct_event_ids = int(dedup_check["distinct_event_ids"]) if dedup_check else 0
    duplicate_durable_side_effects = max(total_rows - distinct_event_ids, 0)

    # Every entity table that gets created off a specific event_id must also
    # have at most one row per event_id - reusing the same join pattern
    # persistence-smoke.py already uses for this exact invariant.
    entity_duplication = query_all(
        config.postgres_dsn,
        """
        SELECT 'orders' AS table_name, COUNT(*) AS rows,
               COUNT(DISTINCT created_event_id) AS distinct_events
        FROM orders o JOIN demo_run_event_manifest m ON m.event_id = o.created_event_id
        WHERE m.run_id = %s
        UNION ALL
        SELECT 'payments', COUNT(*), COUNT(DISTINCT p.event_id)
        FROM payments p JOIN demo_run_event_manifest m ON m.event_id = p.event_id
        WHERE m.run_id = %s
        UNION ALL
        SELECT 'carts', COUNT(*), COUNT(DISTINCT created_event_id)
        FROM carts c JOIN demo_run_event_manifest m ON m.event_id = c.created_event_id
        WHERE m.run_id = %s
        """,
        (result.run_id, result.run_id, result.run_id),
    )
    entity_duplicate_rows = sum(
        max(int(row["rows"]) - int(row["distinct_events"]), 0)
        for row in entity_duplication
    )

    return {
        "run_id": result.run_id,
        "status": result.status,
        "requested_event_count": event_count,
        "seed": seed,
        "total_deliveries": total_deliveries,
        "unique_event_ids": unique_event_ids,
        "duplicate_deliveries_reported_by_processor": duplicate_count_reported,
        "duplicate_deliveries_implied": max(total_deliveries - unique_event_ids, 0),
        "processed_events_total_rows": total_rows,
        "processed_events_distinct_event_ids": distinct_event_ids,
        "duplicate_durable_side_effects_processed_events": (
            duplicate_durable_side_effects
        ),
        "duplicate_durable_side_effects_entity_tables": entity_duplicate_rows,
        "entity_table_breakdown": entity_duplication,
        "verification": "PASS"
        if duplicate_durable_side_effects == 0 and entity_duplicate_rows == 0
        else "FAIL",
        "captured_at": now_iso(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--repeat-index", type=int, required=True)
    parser.add_argument("--event-count", type=int, default=200)
    parser.add_argument("--events-per-second", type=int, default=50)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    config = load_config(args.run_tag)
    api = DemoApiClient(config.demo_api_base_url)
    seed = (
        args.seed
        if args.seed is not None
        else derive_seed(config.run_tag, "idempotency", str(args.repeat_index))
    )
    result = run_once(
        config,
        api,
        event_count=args.event_count,
        events_per_second=args.events_per_second,
        seed=seed,
    )
    out_path = phase_path(config.phase_dir(), f"idempotency_run{args.repeat_index}")
    write_json(out_path, result)
    print(f"wrote {out_path}")
    dup_side_effects = result["duplicate_durable_side_effects_processed_events"]
    print(
        f"deliveries={result['total_deliveries']} unique={result['unique_event_ids']} "
        f"duplicate_side_effects={dup_side_effects} "
        f"verification={result['verification']}"
    )
    return 0 if result["verification"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
