"""Structured JSON logging utilities."""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict


_REQUIRED_FIELDS = ("ts", "level", "msg", "run_id", "stage", "source", "doc_id", "doi", "pmid")


class JsonFormatter(logging.Formatter):
    """Formatter that serialises log records to JSON with the required fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "msg": record.getMessage(),
            "run_id": getattr(record, "run_id", None),
            "stage": getattr(record, "stage", None),
            "source": getattr(record, "source", None),
            "doc_id": getattr(record, "doc_id", None),
            "doi": getattr(record, "doi", None),
            "pmid": getattr(record, "pmid", None),
        }
        extra = getattr(record, "extra_fields", {})
        if isinstance(extra, dict):
            payload.update({k: v for k, v in extra.items() if k not in payload})
        for field in _REQUIRED_FIELDS:
            payload.setdefault(field, None)
        return json.dumps(payload, ensure_ascii=False)


class ContextLogger(logging.LoggerAdapter):
    """Logger adapter injecting structured context into log records."""

    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        extra = kwargs.setdefault("extra", {})
        context = {**self.extra}
        supplied = extra.get("extra_fields", {})
        if supplied:
            context.setdefault("extra_fields", {}).update(supplied)
        for field in ("run_id", "stage", "source", "doc_id", "doi", "pmid"):
            extra.setdefault(field, context.get(field))
        if "extra_fields" in context:
            extra.setdefault("extra_fields", context["extra_fields"])
        return msg, kwargs


_LOGGING_CONFIGURED = False


def setup_logging(level: int = logging.INFO) -> None:
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = [handler]
    _LOGGING_CONFIGURED = True


def get_logger(run_id: str, stage: str | None = None, source: str | None = None, **context: Any) -> ContextLogger:
    setup_logging()
    base_logger = logging.getLogger("project")
    extra_context = {
        "run_id": run_id,
        "stage": stage,
        "source": source,
    }
    extra_context.update(context)
    return ContextLogger(base_logger, extra_context)


def get_error_logger(source: str) -> logging.Logger:
    setup_logging()
    return logging.getLogger(f"project.{source}.error")

