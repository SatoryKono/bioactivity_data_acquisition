"""Logging helpers for the ETL pipeline."""

from __future__ import annotations

import logging
from typing import Any, cast

import structlog
from structlog.stdlib import BoundLogger


def configure_logging(level: str = "INFO") -> BoundLogger:
    """Configure structlog for structured logging."""

    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )
    logger = structlog.get_logger()
    return cast(BoundLogger, logger)


def bind_stage(logger: BoundLogger, stage: str, **extra: Any) -> BoundLogger:
    """Attach contextual metadata to the logger."""

    return logger.bind(stage=stage, **extra)


__all__ = ["bind_stage", "configure_logging"]
