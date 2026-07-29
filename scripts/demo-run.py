#!/usr/bin/env python3
"""Create and boundedly poll one allow-listed demo run through the API."""

import argparse
import json
import sys
import time
import urllib.request
from typing import cast


def request(
    url: str, method: str = "GET", body: dict[str, object] | None = None
) -> dict[str, object]:
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return cast(dict[str, object], json.load(response))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario")
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--rate", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--base-url", default="http://127.0.0.1:8082")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    body: dict[str, object] = {
        "scenario_type": args.scenario,
        "event_count": args.count,
        "events_per_second": args.rate,
        "seed": args.seed,
    }
    if args.scenario == "mixed_traffic":
        body["persona_distribution"] = {
            "normal": 50,
            "suspicious": 20,
            "bot": 10,
            "account_takeover": 10,
            "discount_hunter": 5,
            "indecisive": 5,
        }
    run = request(f"{args.base_url}/api/v1/runs", "POST", body)
    run_id = str(run["run_id"])
    print(run_id)
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        current = request(f"{args.base_url}/api/v1/runs/{run_id}")
        if current["status"] in {"COMPLETED", "STOPPED", "FAILED"}:
            print(
                json.dumps(
                    request(f"{args.base_url}/api/v1/runs/{run_id}/summary"), indent=2
                )
            )
            return 0 if current["status"] == "COMPLETED" else 1
        time.sleep(1)
    print("run polling timed out", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
