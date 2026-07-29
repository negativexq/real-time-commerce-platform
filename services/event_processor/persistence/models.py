"""Typed persistence results shared by repositories and orchestration."""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PersistenceResult:
    """Outcome of one committed PostgreSQL event transaction."""

    already_persisted: bool
    affected_tables: tuple[str, ...] = ()
    rows_written: dict[str, int] = field(default_factory=dict)
    duration_ms: float = 0
