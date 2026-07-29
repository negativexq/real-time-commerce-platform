"""Persistence registry completeness and canonical ledger hash tests."""

from services.event_processor.persistence.handlers import (
    default_persistence_registry,
)
from services.event_processor.persistence.repositories.processed_events import (
    canonical_payload_hash,
)
from shared.commerce_common.enums import EventType
from tests.unit.test_processor_validation import event


def test_persistence_registry_covers_every_shared_event_type() -> None:
    assert set(default_persistence_registry()) == set(EventType)


def test_canonical_payload_hash_is_stable_and_content_sensitive() -> None:
    original = event()
    assert canonical_payload_hash(original) == canonical_payload_hash(original)
    changed = original.model_copy(update={"source": "different"})
    assert canonical_payload_hash(original) != canonical_payload_hash(changed)
