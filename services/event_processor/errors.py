"""Explicit processor error taxonomy."""


class ProcessorError(RuntimeError):
    """Base processor error."""


class PermanentMessageError(ProcessorError):
    """A source record cannot become valid through retry."""


class RetryableProcessingError(ProcessorError):
    """A transient per-message operation may succeed on retry."""


class PermanentProcessingError(ProcessorError):
    """A deterministic handler or processor configuration rejection."""


class FatalInfrastructureError(ProcessorError):
    """An unrecoverable application infrastructure failure."""


class RetryableDatabaseError(RetryableProcessingError):
    """A transient database operation may succeed on retry."""


class MissingBusinessDependencyError(RetryableProcessingError):
    """A valid child event arrived before its required durable parent."""


class PermanentDatabaseIntegrityError(PermanentProcessingError):
    """A deterministic durable-data conflict cannot be retried safely."""


class AlreadyPersistedEvent(ProcessorError):
    """The identical durable event and its business effects already committed."""


class StartupDatabaseError(FatalInfrastructureError):
    """PostgreSQL or the required schema is unavailable at startup."""


class FraudContextDependencyError(MissingBusinessDependencyError):
    """Required persisted fraud history is temporarily unavailable."""


class FraudRuleConfigurationError(FatalInfrastructureError):
    """Fraud rules or thresholds are invalid at startup."""


class FraudEvaluationIntegrityError(PermanentProcessingError):
    """A deterministic fraud evaluation conflicts with durable state."""


class FraudOutboxRetryableError(RetryableProcessingError):
    """A transient outbox database or Kafka operation failed."""


class FraudOutboxPermanentError(PermanentProcessingError):
    """A stored outbox event is irrecoverably invalid."""
