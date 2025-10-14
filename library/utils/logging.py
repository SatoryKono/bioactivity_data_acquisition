"""Structured logging configuration for the ETL pipeline."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable

import structlog


def _error_file_writer(log_dir: Path) -> Callable[[Any, str, dict[str, Any]], dict[str, Any]]:
    """Create a structlog processor that mirrors error events into ``*.error`` files."""

    log_dir.mkdir(parents=True, exist_ok=True)

    def processor(_: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        if method_name in {"error", "exception", "critical"}:
            source = event_dict.get("source") or "pipeline"
            path = log_dir / f"{source}.error"
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event_dict, ensure_ascii=False) + "\n")
        return event_dict

    return processor


def setup_logging(
    level: int | str = "INFO", log_dir: str | Path | None = None
) -> structlog.stdlib.BoundLogger:
    """Initialise structlog with JSON output and error file mirroring."""

    level_value = logging.getLevelName(level) if isinstance(level, str) else level
    if isinstance(level_value, str):
        # getLevelName returns the textual form if level not found; fall back to INFO
        level_value = logging.INFO

    target_dir = Path(log_dir or "logs")
    target_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=level_value,
        format="%(message)s",
        handlers=[logging.StreamHandler()],
        force=True,
    )

    structlog.reset_defaults()
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso", key="ts"),
            structlog.stdlib.add_log_level,
            structlog.processors.EventRenamer("msg"),
            _error_file_writer(target_dir),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger()


def get_logger(**initial_context: Any) -> structlog.stdlib.BoundLogger:
    """Return a logger bound with the provided context."""

    logger = structlog.get_logger()
    if initial_context:
        logger = logger.bind(**initial_context)
    return logger
