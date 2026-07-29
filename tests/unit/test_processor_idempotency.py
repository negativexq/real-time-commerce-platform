"""Redis idempotency state-machine tests at the script boundary."""

import json
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from services.event_processor.config import ProcessorConfig
from services.event_processor.idempotency import (
    COMPLETE_SCRIPT,
    RELEASE_SCRIPT,
    RedisIdempotencyStore,
    ReservationState,
)
from services.event_processor.models import ConsumedMessage

EVENT_ID = UUID("00000000-0000-4000-8000-000000000001")
NOW = datetime(2026, 1, 1, tzinfo=UTC)


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    def eval(self, script: str, numkeys: int, *args: object) -> object:
        assert numkeys == 1
        key = str(args[0])
        if script == COMPLETE_SCRIPT:
            current = json.loads(self.values.get(key, "{}"))
            if current.get("status") != "processing" or current.get("token") != args[1]:
                return 0
            self.values[key] = str(args[2])
            self.ttls[key] = cast(int, args[3])
            return 1
        if script == RELEASE_SCRIPT:
            current = json.loads(self.values.get(key, "{}"))
            if current.get("status") != "processing" or current.get("token") != args[1]:
                return 0
            self.values.pop(key, None)
            self.ttls.pop(key, None)
            return 1
        if key not in self.values:
            self.values[key] = str(args[1])
            self.ttls[key] = cast(int, args[2])
            return ["reserved", str(args[1])]
        return [json.loads(self.values[key])["status"], self.values[key]]

    def ping(self) -> bool:
        return True

    def close(self) -> None:
        pass


def record() -> ConsumedMessage:
    return ConsumedMessage("commerce.events", 0, 1, NOW, b"k", b"v", [])


def test_reserve_complete_and_completed_duplicate() -> None:
    fake = FakeRedis()
    config = ProcessorConfig(
        processor_idempotency_processing_ttl_seconds=10,
        processor_idempotency_completed_ttl_seconds=100,
        processor_idempotency_key_prefix="test:processor",
    )
    store = RedisIdempotencyStore(config, fake)
    reservation = store.reserve(EVENT_ID, "owner", record(), NOW)
    assert reservation.state is ReservationState.RESERVED
    assert fake.ttls[store.key_for(EVENT_ID)] == 10
    assert store.complete(EVENT_ID, "owner", NOW)
    assert fake.ttls[store.key_for(EVENT_ID)] == 100
    duplicate = store.reserve(EVENT_ID, "other", record(), NOW)
    assert duplicate.state is ReservationState.COMPLETED


def test_active_lease_and_wrong_token_cannot_mutate() -> None:
    fake = FakeRedis()
    store = RedisIdempotencyStore(ProcessorConfig(), fake)
    store.reserve(EVENT_ID, "owner", record(), NOW)
    active = store.reserve(EVENT_ID, "other", record(), NOW)
    assert active.state is ReservationState.PROCESSING
    assert not store.complete(EVENT_ID, "wrong", NOW)
    assert not store.release(EVENT_ID, "wrong")
    assert store.release(EVENT_ID, "owner")
    assert store.reserve(EVENT_ID, "new-owner", record(), NOW).state is (
        ReservationState.RESERVED
    )


def test_namespaced_event_id_key() -> None:
    store = RedisIdempotencyStore(
        ProcessorConfig(processor_idempotency_key_prefix="commerce:test:v1"),
        FakeRedis(),
    )
    assert store.key_for(EVENT_ID) == f"commerce:test:v1:{EVENT_ID}"
