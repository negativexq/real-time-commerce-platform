"""Redis-backed atomic event reservation and completion."""

import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from time import perf_counter
from typing import Protocol
from uuid import UUID

from redis import Redis
from redis.exceptions import RedisError

from services.event_processor.config import ProcessorConfig
from services.event_processor.errors import RetryableProcessingError
from services.event_processor.models import ConsumedMessage
from shared.observability.metrics import ApplicationMetrics

RESERVE_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2], 'NX')
  return {'reserved', ARGV[1]}
end
local parsed = cjson.decode(current)
if parsed.status == 'completed' then return {'completed', current} end
return {'processing', current}
"""

COMPLETE_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local parsed = cjson.decode(current)
if parsed.status ~= 'processing' or parsed.token ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
return 1
"""

RELEASE_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local parsed = cjson.decode(current)
if parsed.status ~= 'processing' or parsed.token ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
"""


class RedisClient(Protocol):
    def eval(self, script: str, numkeys: int, *keys_and_args: object) -> object: ...

    def ping(self) -> object: ...

    def close(self) -> None: ...


class ReservationState(StrEnum):
    RESERVED = "reserved"
    COMPLETED = "completed"
    PROCESSING = "processing"


@dataclass(frozen=True, slots=True)
class Reservation:
    state: ReservationState
    token: str | None
    existing: dict[str, object] | None = None


class RedisIdempotencyStore:
    """Token-guarded Redis leases; event payloads are never stored."""

    def __init__(
        self,
        config: ProcessorConfig,
        client: RedisClient | None = None,
        metrics: ApplicationMetrics | None = None,
    ) -> None:
        self._config = config
        self._client = client or Redis.from_url(
            config.redis_url,
            socket_timeout=config.processor_redis_socket_timeout_seconds,
            socket_connect_timeout=(config.processor_redis_connect_timeout_seconds),
            decode_responses=True,
        )
        self._metrics = metrics

    def key_for(self, event_id: UUID) -> str:
        return f"{self._config.processor_idempotency_key_prefix}:{event_id}"

    def ping(self) -> None:
        try:
            self._client.ping()
        except RedisError as exc:
            raise RetryableProcessingError("Redis is unavailable") from exc

    def reserve(
        self,
        event_id: UUID,
        token: str,
        message: ConsumedMessage,
        first_seen_at: datetime,
    ) -> Reservation:
        started = perf_counter()
        value = json.dumps(
            {
                "status": "processing",
                "token": token,
                "consumer_group": self._config.processor_consumer_group,
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "first_seen_at": first_seen_at.isoformat(),
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        try:
            raw = self._client.eval(
                RESERVE_SCRIPT,
                1,
                self.key_for(event_id),
                value,
                self._config.processor_idempotency_processing_ttl_seconds,
            )
        except RedisError as exc:
            self._observe("reserve", "unavailable", started)
            raise RetryableProcessingError("Redis reservation failed") from exc
        if not isinstance(raw, (list, tuple)) or len(raw) != 2:
            raise RetryableProcessingError("Redis reservation returned invalid data")
        state = ReservationState(_text(raw[0]))
        existing_raw = _text(raw[1])
        existing = (
            json.loads(existing_raw) if state is not ReservationState.RESERVED else None
        )
        result = {
            ReservationState.RESERVED: "reserved",
            ReservationState.COMPLETED: "duplicate_completed",
            ReservationState.PROCESSING: "duplicate_processing",
        }[state]
        self._observe("reserve", result, started)
        return Reservation(
            state,
            token if state is ReservationState.RESERVED else None,
            existing,
        )

    def complete(self, event_id: UUID, token: str, completed_at: datetime) -> bool:
        started = perf_counter()
        value = json.dumps(
            {
                "status": "completed",
                "consumer_group": self._config.processor_consumer_group,
                "completed_at": completed_at.isoformat(),
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        try:
            result = self._client.eval(
                COMPLETE_SCRIPT,
                1,
                self.key_for(event_id),
                token,
                value,
                self._config.processor_idempotency_completed_ttl_seconds,
            )
        except RedisError as exc:
            self._observe("complete", "unavailable", started)
            raise RetryableProcessingError("Redis completion failed") from exc
        completed = bool(result)
        self._observe(
            "complete", "completed" if completed else "token_mismatch", started
        )
        return completed

    def release(self, event_id: UUID, token: str) -> bool:
        started = perf_counter()
        try:
            result = self._client.eval(RELEASE_SCRIPT, 1, self.key_for(event_id), token)
        except RedisError as exc:
            self._observe("reconcile", "unavailable", started)
            raise RetryableProcessingError("Redis release failed") from exc
        released = bool(result)
        self._observe(
            "reconcile", "released" if released else "token_mismatch", started
        )
        return released

    def _observe(self, operation: str, result: str, started: float) -> None:
        if self._metrics is None:
            return
        duration = perf_counter() - started
        self._metrics.idempotency_operations.labels(operation, result).inc()
        self._metrics.idempotency_duration.labels(operation, result).observe(duration)
        self._metrics.processor_redis_duration.labels(operation, result).observe(
            duration
        )

    def close(self) -> None:
        self._client.close()


def _text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)
