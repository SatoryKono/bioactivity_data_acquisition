"""Structured logging helpers for the publications pipeline."""

from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any

import structlog

_RUN_ID: str | None = None
_ERROR_DIR: Path | None = None
_DEFAULT_FIELDS = {
    "source": None,
    "stage": None,
    "doc_id": None,
    "doi": None,
    "pmid": None,
}


def configure_logging(
    level: str = "INFO",
    *,
    processors: list[structlog.types.Processor] | None = None,
    run_id: str | None = None,
    error_dir: Path | None = None,
) -> str:
    """Configure structlog and stdlib logging.

    Returns the ``run_id`` used for the session so callers can include it in artefacts.
    """

    global _RUN_ID, _ERROR_DIR
    _RUN_ID = run_id or uuid.uuid4().hex
    _ERROR_DIR = error_dir or Path("logs")
    _ERROR_DIR.mkdir(parents=True, exist_ok=True)

    normalized_level = level.upper()
    logging.basicConfig(level=getattr(logging, normalized_level, logging.INFO), format="%(message)s")

    structlog.configure(
        cache_logger_on_first_use=True,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        processors=processors
        or [
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso", key="ts"),
            _inject_run_id,
            structlog.processors.add_log_level(key="level"),
            structlog.processors.EventRenamer("msg"),
            _ensure_default_fields,
            structlog.processors.JSONRenderer(sort_keys=True),
        ],
    )
    return _RUN_ID


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a structured logger bound to the provided name."""

    logger = structlog.get_logger(name)
    if _RUN_ID:
        logger = logger.bind(run_id=_RUN_ID)
    return logger


def bind_context(logger: structlog.stdlib.BoundLogger, **context: Any) -> structlog.stdlib.BoundLogger:
    """Return a logger with additional contextual information bound."""

    context_with_defaults = {key: value for key, value in context.items() if value is not None}
    return logger.bind(**context_with_defaults)


def log_error_to_file(source: str, payload: dict[str, Any]) -> None:
    """Write structured error payloads to ``<source>.error`` files."""

    if not source:
        source = "pipeline"
    target_dir = _ERROR_DIR or Path("logs")
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{source}.error"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.write("\n")


def _inject_run_id(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    if _RUN_ID and "run_id" not in event_dict:
        event_dict["run_id"] = _RUN_ID
    return event_dict


def _ensure_default_fields(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    for key, default in _DEFAULT_FIELDS.items():
        event_dict.setdefault(key, default)
    return event_dict
