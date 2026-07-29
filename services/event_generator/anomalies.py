"""Controlled anomaly injection isolated at the serialized Kafka boundary."""

import json
import random
from datetime import timedelta

from services.event_generator.config import GeneratorConfig
from services.event_generator.messages import AnomalyType, PublishableMessage
from services.event_generator.producer import message_headers, message_key
from shared.schemas import EventEnvelope, canonical_json
from shared.schemas.base import ContractModel


def valid_message(event: EventEnvelope[ContractModel]) -> PublishableMessage:
    """Convert one validated event into a Kafka-ready message."""
    return PublishableMessage(
        canonical_json(event).encode(),
        message_key(event),
        message_headers(event),
        event.event_id,
        event.event_type.value,
        event.correlation_id,
    )


class AnomalyInjector:
    """Create bounded raw mutations without weakening typed contracts."""

    def __init__(self, config: GeneratorConfig, random_source: random.Random) -> None:
        self._config = config
        self._random = random_source

    def prepare(
        self,
        events: tuple[EventEnvelope[ContractModel], ...],
    ) -> list[PublishableMessage]:
        """Return valid messages plus selected bounded anomalous records."""
        messages = [valid_message(event) for event in events]
        valid_messages = list(messages)
        if not self._config.generator_anomalies_enabled:
            return messages

        candidates = [
            (AnomalyType.DUPLICATE, self._config.generator_duplicate_event_probability),
            (
                AnomalyType.MALFORMED_JSON,
                self._config.generator_malformed_json_probability,
            ),
            (
                AnomalyType.MISSING_FIELD,
                self._config.generator_missing_field_probability,
            ),
            (
                AnomalyType.UNKNOWN_EVENT_TYPE,
                self._config.generator_unknown_event_type_probability,
            ),
            (AnomalyType.LATE_EVENT, self._config.generator_late_event_probability),
            (
                AnomalyType.PAYLOAD_MISMATCH,
                self._config.generator_payload_mismatch_probability,
            ),
        ]
        emitted = 0
        for index, (kind, probability) in enumerate(candidates):
            if emitted >= self._config.generator_max_anomalies_per_journey:
                break
            if probability and self._random.random() < probability:
                source = valid_messages[index % len(valid_messages)]
                messages.append(self._mutate(kind, source, valid_messages))
                emitted += 1

        if (
            emitted < self._config.generator_max_anomalies_per_journey
            and len(messages) >= 2
            and self._config.generator_out_of_order_probability
            and self._random.random() < self._config.generator_out_of_order_probability
        ):
            first, second = messages[0], messages[1]
            messages[0] = self._tag(second, AnomalyType.OUT_OF_ORDER)
            messages[1] = self._tag(first, AnomalyType.OUT_OF_ORDER)
        return messages

    def _mutate(
        self,
        kind: AnomalyType,
        source: PublishableMessage,
        all_messages: list[PublishableMessage],
    ) -> PublishableMessage:
        if kind is AnomalyType.DUPLICATE:
            return self._tag(source, kind)
        if kind is AnomalyType.MALFORMED_JSON:
            return self._replace(source, source.value[:-1], kind)

        decoded = json.loads(source.value)
        if kind is AnomalyType.MISSING_FIELD:
            decoded.pop("event_id", None)
        elif kind is AnomalyType.UNKNOWN_EVENT_TYPE:
            decoded["event_type"] = "synthetic_unknown_event"
        elif kind is AnomalyType.PAYLOAD_MISMATCH:
            replacement = next(
                (
                    json.loads(message.value)
                    for message in all_messages
                    if message.event_type != source.event_type
                    and message.anomaly_type is None
                ),
                None,
            )
            if replacement is not None:
                decoded["payload"] = replacement["payload"]
            else:
                decoded["payload"] = {}
        elif kind is AnomalyType.LATE_EVENT:
            from shared.schemas import parse_event

            parsed = parse_event(source.value)
            seconds = min(self._config.generator_max_late_event_seconds, 240)
            decoded["event_time"] = (
                (parsed.event_time - timedelta(seconds=seconds))
                .isoformat()
                .replace("+00:00", "Z")
            )
        value = json.dumps(
            decoded, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode()
        return self._replace(source, value, kind)

    @staticmethod
    def _replace(
        source: PublishableMessage,
        value: bytes,
        kind: AnomalyType,
    ) -> PublishableMessage:
        return PublishableMessage(
            value,
            source.key,
            [*source.headers, ("synthetic_anomaly", kind.value.encode())],
            source.event_id,
            source.event_type,
            source.correlation_id,
            kind,
        )

    @classmethod
    def _tag(
        cls,
        source: PublishableMessage,
        kind: AnomalyType,
    ) -> PublishableMessage:
        return cls._replace(source, source.value, kind)
