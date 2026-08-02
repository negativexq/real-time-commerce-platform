"""Transactional outbox verification.

Drives fraud-triggering scenarios through the real primary pipeline so the
real fraud engine and fraud-outbox-publisher create and drain
``fraud_alerts``/``fraud_outbox`` rows, then verifies durable outbox state
directly in Postgres - no isolation needed since this is read-only
verification of the real production outbox.
"""

import argparse
import sys
import time
from typing import Any

from scripts.benchmark.artifacts import now_iso, phase_path, write_json
from scripts.benchmark.config import BenchmarkConfig, derive_seed, load_config
from scripts.benchmark.demo_api import DemoApiClient
from scripts.benchmark.pg import query_all, query_one
from scripts.benchmark.stats import percentiles

FRAUD_OUTBOX_MAX_ATTEMPTS = 10  # services/event_processor/fraud/config.py default
FRAUD_OUTBOX_CLAIM_TTL_SECONDS = 30


def _wait_for_drain(dsn: str, run_id: str, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        row = query_one(
            dsn,
            """
            SELECT COUNT(*) AS pending
            FROM fraud_alerts fa
            JOIN demo_run_event_manifest m ON m.event_id = fa.source_event_id
            JOIN fraud_outbox fo ON fo.aggregate_id = fa.alert_id
            WHERE m.run_id = %s AND fo.status IN ('PENDING', 'PUBLISHING')
            """,
            (run_id,),
        )
        if row and int(row["pending"]) == 0:
            return
        time.sleep(1.0)


def run_once(
    config: BenchmarkConfig,
    api: DemoApiClient,
    *,
    scenario_type: str,
    event_count: int,
    events_per_second: int,
    seed: int,
) -> dict[str, Any]:
    body = {
        "scenario_type": scenario_type,
        "event_count": event_count,
        "events_per_second": events_per_second,
        "seed": seed,
        "notes": f"benchmark:{config.run_tag}:outbox:{scenario_type}",
    }
    result = api.run_scenario(
        body, timeout=max(120.0, event_count / max(events_per_second, 1) * 4 + 60)
    )

    _wait_for_drain(config.postgres_dsn, result.run_id, timeout_seconds=60.0)

    alerts = query_one(
        config.postgres_dsn,
        """
        SELECT COUNT(*) AS alert_count
        FROM fraud_alerts fa
        JOIN demo_run_event_manifest m ON m.event_id = fa.source_event_id
        WHERE m.run_id = %s
        """,
        (result.run_id,),
    )
    alert_count = int(alerts["alert_count"]) if alerts else 0

    outbox_rows = query_all(
        config.postgres_dsn,
        """
        SELECT fo.status,
               fo.attempts,
               EXTRACT(EPOCH FROM fo.created_at) * 1000 AS created_at_ms,
               EXTRACT(EPOCH FROM fo.published_at) * 1000 AS published_at_ms
        FROM fraud_alerts fa
        JOIN demo_run_event_manifest m ON m.event_id = fa.source_event_id
        JOIN fraud_outbox fo ON fo.aggregate_id = fa.alert_id
        WHERE m.run_id = %s
        """,
        (result.run_id,),
    )
    outbox_count = len(outbox_rows)
    status_counts: dict[str, int] = {}
    for row in outbox_rows:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1

    published_delays_ms = [
        float(row["published_at_ms"]) - float(row["created_at_ms"])
        for row in outbox_rows
        if row["status"] == "PUBLISHED"
        and row["published_at_ms"]
        and row["created_at_ms"]
    ]
    delay_stats = percentiles(published_delays_ms)
    delay_stats["sample_count"] = len(published_delays_ms)

    stuck_rows = [
        row
        for row in outbox_rows
        if row["status"] in ("PENDING", "PUBLISHING")
        and not (
            row["status"] == "FAILED" and row["attempts"] >= FRAUD_OUTBOX_MAX_ATTEMPTS
        )
    ]
    missing_outbox_rows = max(alert_count - outbox_count, 0)
    no_lost_alert = missing_outbox_rows == 0 and len(stuck_rows) == 0

    return {
        "scenario_type": scenario_type,
        "run_id": result.run_id,
        "requested_event_count": event_count,
        "seed": seed,
        "fraud_alerts_created": alert_count,
        "outbox_rows_created": outbox_count,
        "outbox_status_breakdown": status_counts,
        "outbox_published_count": status_counts.get("PUBLISHED", 0),
        "outbox_pending_or_failed_count": outbox_count
        - status_counts.get("PUBLISHED", 0),
        "publish_success_rate": (
            status_counts.get("PUBLISHED", 0) / outbox_count if outbox_count else None
        ),
        "publish_delay_ms": delay_stats,
        "missing_outbox_rows_for_alerts": missing_outbox_rows,
        "stuck_pending_or_publishing_rows": len(stuck_rows),
        "no_committed_alert_silently_lost": no_lost_alert,
        "verification": "PASS" if no_lost_alert else "FAIL",
        "captured_at": now_iso(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--repeat-index", type=int, required=True)
    parser.add_argument("--scenario-type", default="suspicious_payment")
    parser.add_argument("--event-count", type=int, default=150)
    parser.add_argument("--events-per-second", type=int, default=30)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    config = load_config(args.run_tag)
    api = DemoApiClient(config.demo_api_base_url)
    seed = (
        args.seed
        if args.seed is not None
        else derive_seed(
            config.run_tag, "outbox", args.scenario_type, str(args.repeat_index)
        )
    )
    result = run_once(
        config,
        api,
        scenario_type=args.scenario_type,
        event_count=args.event_count,
        events_per_second=args.events_per_second,
        seed=seed,
    )
    out_path = phase_path(config.phase_dir(), f"outbox_run{args.repeat_index}")
    write_json(out_path, result)
    print(f"wrote {out_path}")
    published = result["outbox_published_count"]
    print(
        f"alerts={result['fraud_alerts_created']} published={published} "
        f"verification={result['verification']}"
    )
    return 0 if result["verification"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
