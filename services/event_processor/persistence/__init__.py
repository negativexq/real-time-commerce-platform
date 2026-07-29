"""Transactional PostgreSQL persistence for validated commerce events."""

from services.event_processor.persistence.database import Database
from services.event_processor.persistence.handlers import default_persistence_registry
from services.event_processor.persistence.unit_of_work import UnitOfWorkFactory

__all__ = ["Database", "UnitOfWorkFactory", "default_persistence_registry"]
