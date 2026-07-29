"""Structured JSON logging shared by processor components."""

import logging
from typing import cast

import structlog
from structlog.typing import FilteringBoundLogger


def configure_logging(level: str) -> None:
    logging.basicConfig(format="%(message)s", level=level, force=True)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(sort_keys=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(level)
        ),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger() -> FilteringBoundLogger:
    return cast(FilteringBoundLogger, structlog.get_logger("event_processor"))
