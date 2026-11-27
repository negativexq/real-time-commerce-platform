"""Outbox publisher error classification."""


class FraudOutboxRetryableError(Exception):
    """A transient outbox database or Kafka operation failed."""
