"""Invariant checks over collected benchmark results.

Every check here maps directly to a correctness requirement from the
benchmark brief (zero duplicate durable side effects, DLQ metadata
completeness, offsets committed only after terminal handling, no silently
lost outbox alert). Writes verification.json with an explicit PASS/FAIL and
observed value per check - never just raw numbers.
"""

import argparse
import os
import sys
from typing import Any

from scripts.benchmark.artifacts import now_iso, read_json, write_json
from scripts.benchmark.config import load_config


def _check(name: str, passed: bool, observed: Any, detail: str) -> dict[str, Any]:
    return {
        "check": name,
        "result": "PASS" if passed else "FAIL",
        "observed": observed,
        "detail": detail,
    }


def verify(run_tag: str) -> dict[str, Any]:
    config = load_config(run_tag)
    phase_dir = config.phase_dir()
    summary_path = os.path.join(phase_dir, "summary.json")
    summary = read_json(summary_path)

    checks: list[dict[str, Any]] = []

    idempotency_runs = summary["idempotency"]["raw_runs"]
    if idempotency_runs:
        total_dup_side_effects = summary["idempotency"][
            "duplicate_durable_side_effects_total"
        ]
        checks.append(
            _check(
                "idempotency.zero_duplicate_durable_side_effects",
                total_dup_side_effects == 0,
                total_dup_side_effects,
                "Sum of duplicate_durable_side_effects_processed_events + "
                "duplicate_durable_side_effects_entity_tables across all "
                "idempotency runs must be exactly 0.",
            )
        )
        for run in idempotency_runs:
            checks.append(
                _check(
                    f"idempotency.run[{run.get('run_id')}].duplicates_actually_injected",
                    run.get("duplicate_deliveries_implied", 0) > 0,
                    run.get("duplicate_deliveries_implied"),
                    "The duplicate_delivery scenario must have actually produced "
                    "duplicate deliveries (otherwise the test proves nothing).",
                )
            )
    else:
        checks.append(
            _check(
                "idempotency.zero_duplicate_durable_side_effects",
                False,
                None,
                "no idempotency runs found",
            )
        )

    malformed = summary["retry_dlq"]["malformed"]
    if malformed:
        checks.append(
            _check(
                "retry_dlq.malformed_dlq_metadata_complete",
                malformed.get("all_metadata_complete", False),
                malformed.get("all_metadata_complete"),
                "Every DLQ record produced by the malformed-event scenarios "
                "must contain all required DlqEnvelope fields.",
            )
        )
        for case in malformed.get("cases", []):
            checks.append(
                _check(
                    f"retry_dlq.malformed[{case['malformed_case']}].reached_dlq",
                    case.get("dlq_records_from_kafka", 0) > 0,
                    case.get("dlq_records_from_kafka"),
                    "Each malformed case must actually produce at least one "
                    "DLQ record.",
                )
            )

    retry_runs = summary["retry_dlq"]["retry"]["raw_runs"]
    for run in retry_runs:
        checks.append(
            _check(
                f"retry_dlq.retry[{run.get('isolated_consumer_group')}].offsets_committed_only_after_terminal",
                run.get("offsets_committed_only_after_terminal_handling", False),
                {
                    "committed_offset_total": run.get("committed_offset_total"),
                    "terminal_outcomes": run.get("terminal_outcomes"),
                },
                "Committed offset total must never exceed the log end offset "
                "and must be consistent with the number of terminally "
                "handled records - i.e. no offset is committed ahead of "
                "terminal (success/DLQ) handling.",
            )
        )
        checks.append(
            _check(
                f"retry_dlq.retry[{run.get('isolated_consumer_group')}].dlq_metadata_complete",
                run.get("dlq_metadata_complete", False),
                run.get("dlq_metadata_complete"),
                "DLQ records produced by exhausted retries must contain all "
                "required DlqEnvelope fields.",
            )
        )
        checks.append(
            _check(
                f"retry_dlq.retry[{run.get('isolated_consumer_group')}].retry_then_success_observed",
                run.get("retry_success_count", 0) > 0,
                run.get("retry_success_count"),
                "The retry sub-test must have actually observed at least one "
                "event succeed after a controlled transient failure.",
            )
        )

    outbox_runs = summary["outbox"]["raw_runs"]
    for run in outbox_runs:
        checks.append(
            _check(
                f"outbox.run[{run.get('run_id')}].no_committed_alert_silently_lost",
                run.get("no_committed_alert_silently_lost", False),
                {
                    "missing_outbox_rows_for_alerts": run.get(
                        "missing_outbox_rows_for_alerts"
                    ),
                    "stuck_pending_or_publishing_rows": run.get(
                        "stuck_pending_or_publishing_rows"
                    ),
                },
                "Every fraud_alerts row must have a corresponding fraud_outbox "
                "row, and every outbox row must reach PUBLISHED or terminal "
                "FAILED by the end of the drain window.",
            )
        )

    lag_burst = summary["consumer_lag"]["burst"]
    if lag_burst:
        checks.append(
            _check(
                "consumer_lag.burst_baseline_measured",
                lag_burst.get("baseline_lag") is not None,
                lag_burst.get("baseline_lag"),
                "Baseline consumer lag must be a real measured value (0 or "
                "positive), not missing.",
            )
        )

    lag_outage = summary["consumer_lag"]["outage"]
    if lag_outage:
        checks.append(
            _check(
                "consumer_lag.outage_recovered_within_timeout",
                lag_outage.get("recovered_within_timeout", False),
                lag_outage.get("recovery_time_seconds"),
                "After the event-processor container is restarted, lag must "
                "return to baseline within the poll timeout (300s).",
            )
        )

    all_passed = all(c["result"] == "PASS" for c in checks)
    return {
        "run_tag": run_tag,
        "generated_at": now_iso(),
        "checks": checks,
        "all_passed": all_passed,
        "failed_checks": [c["check"] for c in checks if c["result"] == "FAIL"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    args = parser.parse_args()
    result = verify(args.run_tag)
    config = load_config(args.run_tag)
    out_path = os.path.join(config.phase_dir(), "verification.json")
    write_json(out_path, result)
    print(f"wrote {out_path}")
    for check in result["checks"]:
        print(f"  [{check['result']}] {check['check']}")
    return 0 if result["all_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
