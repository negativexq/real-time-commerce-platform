#!/usr/bin/env python3
"""Isolated, API-driven account-takeover acceptance verification."""

import argparse
import json
import time
import urllib.request
from typing import Any, cast
from uuid import UUID

import psycopg
from confluent_kafka import Consumer, KafkaError  # type: ignore[import-untyped]
from psycopg.rows import dict_row

TERMINAL = {"COMPLETED", "FAILED", "STOPPED"}


def api(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    body: dict[str, object] | None = None,
) -> dict[str, Any]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return cast(dict[str, Any], json.load(response))


def wait_for_assignment(consumer: Consumer, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        consumer.poll(0.5)
        if consumer.assignment():
            return
    raise RuntimeError("fraud-alert verification consumer received no assignment")


def run_row(
    connection: psycopg.Connection[dict[str, Any]], run_id: UUID
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT d.run_id, d.test_scope, d.status, d.generated_event_count,
               count(DISTINCT m.event_id)::int AS manifest_events,
               count(DISTINCT pe.event_id)::int AS processed_events,
               count(DISTINCT fe.evaluation_id)
                   FILTER (WHERE fe.decision = 'BLOCK')::int AS block_count,
               count(DISTINCT fa.alert_id)::int AS alert_count,
               count(DISTINCT fo.outbox_id)
                   FILTER (WHERE fo.status = 'PUBLISHED')::int AS published_count,
               (array_agg(DISTINCT fa.alert_id)
                   FILTER (WHERE fa.alert_id IS NOT NULL))[1] AS alert_id,
               (array_agg(DISTINCT fa.alert_event_id)
                   FILTER (WHERE fa.alert_event_id IS NOT NULL))[1] AS alert_event_id
        FROM demo_runs d
        LEFT JOIN demo_run_event_manifest m ON m.run_id = d.run_id
        LEFT JOIN processed_events pe ON pe.event_id = m.event_id
        LEFT JOIN fraud_evaluations fe ON fe.source_event_id = m.event_id
        LEFT JOIN fraud_alerts fa ON fa.source_event_id = m.event_id
        LEFT JOIN fraud_outbox fo ON fo.aggregate_id = fa.alert_id
        WHERE d.run_id = %s
        GROUP BY d.run_id, d.test_scope, d.status
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError("created run was not found in PostgreSQL")
    return row


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://127.0.0.1:8082")
    parser.add_argument(
        "--postgres-dsn",
        default="postgresql://commerce:commerce_local_dev@127.0.0.1:5432/commerce",
    )
    parser.add_argument("--kafka", default="127.0.0.1:29092")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--seed", type=int, default=202611)
    args = parser.parse_args()

    alert_group = f"commerce-demo-alert-verification-{args.seed}-{time.time_ns()}"
    alert_consumer = Consumer(
        {
            "bootstrap.servers": args.kafka,
            "group.id": alert_group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "broker.address.family": "v4",
        }
    )
    alert_consumer.subscribe(["commerce.fraud-alerts"])
    wait_for_assignment(alert_consumer, 20)

    created = api(
        args.api_url,
        "/api/v1/runs",
        method="POST",
        body={
            "scenario_type": "account_takeover",
            "event_count": 80,
            "events_per_second": 100,
            "seed": args.seed,
        },
    )
    run_id = UUID(created["run_id"])
    deadline = time.monotonic() + args.timeout
    evidence: dict[str, Any] | None = None
    with psycopg.connect(args.postgres_dsn, row_factory=dict_row) as connection:
        while time.monotonic() < deadline:
            current = api(args.api_url, f"/api/v1/runs/{run_id}")
            evidence = run_row(connection, run_id)
            if (
                current["status"] == "COMPLETED"
                and evidence["block_count"] >= 1
                and evidence["alert_count"] >= 1
                and evidence["published_count"] >= 1
            ):
                break
            if current["status"] in TERMINAL and current["status"] != "COMPLETED":
                raise RuntimeError(f"run ended as {current['status']}")
            time.sleep(1)
        else:
            raise RuntimeError("takeover verification timed out")

    assert evidence is not None
    if not (
        evidence["generated_event_count"]
        == evidence["manifest_events"]
        == evidence["processed_events"]
    ):
        raise RuntimeError("run manifest and durable event counts do not match")
    alert_event_id = str(evidence["alert_event_id"])
    kafka_record: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        message = alert_consumer.poll(1)
        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise RuntimeError(str(message.error()))
        payload = json.loads(message.value())
        if payload.get("event_id") == alert_event_id:
            kafka_record = {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "event_id": payload["event_id"],
                "event_type": payload["event_type"],
            }
            break
    alert_consumer.close()
    if kafka_record is None:
        raise RuntimeError("matching commerce.fraud-alerts record was not observed")

    print(
        json.dumps(
            {"run": evidence, "kafka_record": kafka_record}, default=str, indent=2
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
