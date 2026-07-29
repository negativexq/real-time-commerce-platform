"""Bounded retry tests without real sleeping."""

import random

import pytest

from services.event_processor.errors import (
    PermanentProcessingError,
    RetryableProcessingError,
)
from services.event_processor.retry import RetryPolicy, run_with_retry


def policy(attempts: int = 3) -> RetryPolicy:
    return RetryPolicy(attempts, 100, 250, 2, 0)


def test_backoff_is_exponential_and_capped() -> None:
    rng = random.Random(1)
    assert policy().backoff_seconds(1, rng) == 0.1
    assert policy().backoff_seconds(2, rng) == 0.2
    assert policy().backoff_seconds(3, rng) == 0.25


def test_jitter_is_deterministic_with_injected_rng() -> None:
    jittered = RetryPolicy(3, 100, 500, 2, 0.5)
    assert jittered.backoff_seconds(1, random.Random(5)) == pytest.approx(
        jittered.backoff_seconds(1, random.Random(5))
    )


def test_retry_n_failures_then_success() -> None:
    calls: list[int] = []
    waits: list[float] = []

    def operation(attempt: int) -> str:
        calls.append(attempt)
        if attempt < 3:
            raise RetryableProcessingError("temporary")
        return "ok"

    result, attempts = run_with_retry(
        operation, policy(), waits.append, random.Random(1)
    )
    assert (result, attempts) == ("ok", 3)
    assert calls == [1, 2, 3]
    assert waits == [0.1, 0.2]


def test_retry_exhaustion_and_permanent_failure() -> None:
    with pytest.raises(RetryableProcessingError):
        run_with_retry(
            lambda attempt: (_ for _ in ()).throw(
                RetryableProcessingError(str(attempt))
            ),
            policy(2),
            lambda seconds: None,
            random.Random(1),
        )
    calls = 0

    def permanent(attempt: int) -> None:
        nonlocal calls
        calls += 1
        raise PermanentProcessingError(str(attempt))

    with pytest.raises(PermanentProcessingError):
        run_with_retry(permanent, policy(), lambda seconds: None, random.Random(1))
    assert calls == 1
