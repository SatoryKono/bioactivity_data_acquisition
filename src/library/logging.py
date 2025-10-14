"""Logging configuration utilities for the ETL pipeline."""
from __future__ import annotations

import logging
from pathlib import Path

import structlog


def configure_logging(level: int = logging.INFO, log_path: Path | None = None) -> None:
    """Configure structured logging for the project."""
    processors = [
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ]

    shared_handlers: list[logging.Handler] = []
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(level)
        shared_handlers.append(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    shared_handlers.append(stream_handler)

    logging.basicConfig(level=level, handlers=shared_handlers)

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(level),
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
