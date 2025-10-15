"""Utility helpers for structlog-based logging."""
from __future__ import annotations

import logging
from typing import Any

import structlog

_LOGGING_CONFIGURED = False


def configure_logging(level: int = logging.INFO) -> None:
    """Configure structlog for structured logging.

    The configuration is idempotent – subsequent calls do not reconfigure
    logging to avoid interfering with test log capture.
    """

    global _LOGGING_CONFIGURED

    if _LOGGING_CONFIGURED:
        return

    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(level),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        cache_logger_on_first_use=True,
    )
    _LOGGING_CONFIGURED = True


def get_logger(name: str, **initial_values: Any) -> structlog.BoundLogger:
    """Return a configured structlog logger."""

    if not _LOGGING_CONFIGURED:
        configure_logging()
    return structlog.get_logger(name).bind(**initial_values)
