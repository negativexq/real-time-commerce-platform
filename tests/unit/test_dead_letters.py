"""Replay rules and record rebuilding for stored dead letters."""

import base64
import json
from uuid import UUID

from services.demo_control_api.services.dead_letters import (
    REPLAY_HEADER,
    payload_preview,
    replay_block_reason,
    replay_message,
)

TOPICS = frozenset({"commerce.events"})
RECORD = UUID("00000000-0000-4000-8000-00000000d1a0")


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def test_only_retryable_failures_can_be_replayed() -> None:
    def reason(category: str) -> str | None:
        return replay_block_reason(category, "NEW", True, "commerce.events", TOPICS)

    assert reason("missing_business_dependency") is None
    assert reason("retry_exhausted") is None
    for category in ("malformed_json", "database_integrity_error", "missing_header"):
        assert "deterministic" in (reason(category) or "")


def test_replay_needs_an_open_status_a_full_payload_and_a_known_topic() -> None:
    category = "retry_exhausted"
    assert (
        replay_block_reason(category, "TRIAGED", True, "commerce.events", TOPICS)
        is None
    )
    assert "Already replayed" in (
        replay_block_reason(category, "REPLAYED", True, "commerce.events", TOPICS) or ""
    )
    assert "truncated" in (
        replay_block_reason(category, "NEW", False, "commerce.events", TOPICS) or ""
    )
    assert "commerce.events" in (
        replay_block_reason(category, "NEW", True, "other.topic", TOPICS) or ""
    )


def test_replay_rebuilds_the_original_record_and_marks_it() -> None:
    value = b'{"event_id": "abc"}'
    key, body, headers = replay_message(
        RECORD,
        {
            "source_key_base64": _b64(b"abc"),
            "original_value_base64": _b64(value),
            "source_headers": {"event_type": "order_created", "event_id": "abc"},
        },
    )
    assert key == b"abc"
    assert body == value
    assert headers == [
        ("event_id", b"abc"),
        ("event_type", b"order_created"),
        (REPLAY_HEADER, str(RECORD).encode()),
    ]


def test_replay_accepts_headers_stored_as_a_json_string() -> None:
    _, _, headers = replay_message(
        RECORD,
        {
            "original_value_base64": _b64(b"{}"),
            "source_headers": json.dumps({"source": "generator"}),
        },
    )
    assert ("source", b"generator") in headers


def test_payload_preview_is_readable_and_bounded() -> None:
    preview = payload_preview(_b64(b'{"b": 1, "a": "x"}'), False)
    assert preview["kind"] == "json"
    assert preview["text"].splitlines()[1].strip() == '"a": "x",'
    assert payload_preview(_b64(b"not json"), False)["kind"] == "text"
    assert payload_preview(_b64(b"\xff\xfe"), False)["kind"] == "binary"
    assert payload_preview(None, False)["kind"] == "missing"
    assert payload_preview("%%%", False)["kind"] == "unreadable"
    long = payload_preview(_b64(b"x" * 10_000), False)
    assert len(long["text"]) == 4_000 and long["truncated"] is True
