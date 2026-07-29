"""Bounded retry classification and exponential backoff."""

import random
from collections.abc import Callable
from dataclasses import dataclass

from services.event_processor.errors import RetryableProcessingError


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    maximum_attempts: int
    initial_backoff_ms: int
    maximum_backoff_ms: int
    multiplier: float
    jitter_ratio: float

    def backoff_seconds(self, failed_attempt: int, rng: random.Random) -> float:
        """Return capped backoff after the given one-based failed attempt."""
        base = min(
            self.maximum_backoff_ms,
            self.initial_backoff_ms * self.multiplier ** (failed_attempt - 1),
        )
        jitter = base * self.jitter_ratio
        milliseconds = max(0.0, base + rng.uniform(-jitter, jitter))
        return milliseconds / 1_000


def run_with_retry[T](
    operation: Callable[[int], T],
    policy: RetryPolicy,
    wait: Callable[[float], object],
    rng: random.Random,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> tuple[T, int]:
    """Run an operation until success, permanent failure, or bounded exhaustion."""
    for attempt in range(1, policy.maximum_attempts + 1):
        try:
            return operation(attempt), attempt
        except RetryableProcessingError as exc:
            if attempt == policy.maximum_attempts:
                raise
            if on_retry is not None:
                on_retry(attempt, exc)
            wait(policy.backoff_seconds(attempt, rng))
    raise AssertionError("positive maximum_attempts guarantees loop execution")
