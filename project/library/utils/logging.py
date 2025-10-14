"""Utilities for configuring structured logging across the ETL pipeline."""

from __future__ import annotations

import logging
from typing import Any

import structlog

_DEFAULT_PROCESSORS = [
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.JSONRenderer(),
]


def configure_logging(level: str = "INFO", *, processors: list[structlog.types.Processor] | None = None) -> None:
    """Configure structlog and stdlib logging.

    Parameters
    ----------
    level:
        The minimum logging level to emit.
    processors:
        Optional list of structlog processors. Falls back to a sensible default JSON pipeline.
    """

    normalized_level = level.upper()
    logging.basicConfig(level=getattr(logging, normalized_level, logging.INFO), format="%(message)s")

    structlog.configure(
        cache_logger_on_first_use=True,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        processors=processors or list(_DEFAULT_PROCESSORS),
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a structured logger bound to the provided name."""

    return structlog.get_logger(name)


def bind_context(logger: structlog.stdlib.BoundLogger, **context: Any) -> structlog.stdlib.BoundLogger:
    """Return a logger with additional contextual information bound."""

    return logger.bind(**context)
