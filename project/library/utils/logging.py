"""Logging utilities for structured JSON logs."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import structlog


def configure_logging(run_id: str, level: str = "INFO") -> None:
    """Configure structlog with JSON output.

    Parameters
    ----------
    run_id:
        Identifier for the current pipeline run.
    level:
        Logging level string, defaults to ``INFO``.
    """
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(key="ts", fmt="iso"),
            structlog.processors.add_log_level,
            _inject_run_id(run_id),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    structlog.get_logger().setLevel(level)


def _inject_run_id(run_id: str):
    def processor(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        event_dict.setdefault("run_id", run_id)
        event_dict.setdefault("stage", "")
        event_dict.setdefault("source", "")
        event_dict.setdefault("doc_id", "")
        event_dict.setdefault("doi", "")
        event_dict.setdefault("pmid", "")
        return event_dict

    return processor


def get_logger(**kwargs: Any) -> structlog.stdlib.BoundLogger:
    """Return a logger bound with provided contextual values."""
    return structlog.get_logger().bind(**kwargs)


def write_source_error(source: str, message: str, *, directory: Path | None = None) -> None:
    """Append an error message to the ``<source>.error`` file.

    Parameters
    ----------
    source:
        Name of the upstream source, used as file stem.
    message:
        Message to append.
    directory:
        Optional directory where the error file should be written.
    """
    base_dir = Path(directory or Path.cwd())
    base_dir.mkdir(parents=True, exist_ok=True)
    error_file = base_dir / f"{source}.error"
    with error_file.open("a", encoding="utf-8") as handle:
        handle.write(message.rstrip("\n") + "\n")
