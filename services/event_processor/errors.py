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
