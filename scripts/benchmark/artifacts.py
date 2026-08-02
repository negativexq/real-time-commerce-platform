"""Small helpers for writing/reading benchmark JSON artifacts."""

import json
import os
from datetime import UTC, datetime
from typing import Any


def write_json(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as handle:
        json.dump(payload, handle, indent=2, default=str, sort_keys=True)
        handle.write("\n")


def read_json(path: str) -> dict[str, Any]:
    with open(path) as handle:
        return json.load(handle)


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def phase_path(phase_dir: str, name: str) -> str:
    return os.path.join(phase_dir, f"{name}.json")
