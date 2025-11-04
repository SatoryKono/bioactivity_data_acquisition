"""Utilities for configuring the project wide structured logger.

This module centralises the logging setup so that every service and
command line entry-point uses the exact same configuration.  The
implementation intentionally favours determinism and explicit context
binding so that logs can be consumed by observability pipelines as well
as by golden tests.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable, MutableMapping, Sequence
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Any

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars

__all__ = [
    "LogFormat",
    "LogConfig",
    "DEFAULT_LOG_LEVEL",
    "MANDATORY_FIELDS",
    "configure_logging",
    "bind_global_context",
    "reset_global_context",
    "get_logger",
]


class LogFormat(str, Enum):
    """Supported output formats for the renderer."""

    JSON = "json"
    KEY_VALUE = "key_value"


DEFAULT_LOG_LEVEL = logging.INFO
"""Default log level for the entire application."""

MANDATORY_FIELDS: Sequence[str] = (
    "run_id",
    "pipeline",
    "stage",
    "dataset",
    "component",
    "trace_id",
    "span_id",
)
"""Fields that must always be present in the structured event dictionary."""

_KEY_ORDER: Sequence[str] = (
    "timestamp",
    "level",
    "pipeline",
    "stage",
    "component",
    "dataset",
    "run_id",
    "trace_id",
    "span_id",
    "message",
)


@dataclass(frozen=True, slots=True)
class LogConfig:
    """User configurable logging parameters."""

    level: int | str = DEFAULT_LOG_LEVEL
    format: LogFormat = LogFormat.JSON
    redact_fields: Sequence[str] = ("api_key", "access_token", "password")


def _coerce_log_level(level: int | str) -> int:
    if isinstance(level, int):
        return level
    coerced = logging.getLevelName(level.upper())
    if isinstance(coerced, int):
        return coerced
    raise ValueError(f"Unsupported log level: {level}")


def _redact_sensitive_values(
    _: Any,
    __: str,
    event_dict: MutableMapping[str, Any],
    *,
    redact_fields: Iterable[str],
) -> MutableMapping[str, Any]:
    for field in redact_fields:
        if field in event_dict:
            event_dict[field] = "***REDACTED***"
    return event_dict


def _ensure_mandatory_fields(
    _: Any,
    __: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    missing = [field for field in MANDATORY_FIELDS if field not in event_dict]
    if missing:
        event_dict.setdefault("missing_context", missing)
    return event_dict


def _shared_processors(config: LogConfig) -> list[Any]:
    return [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
        _ensure_mandatory_fields,
        structlog.processors.EventRenamer("message"),
        partial(
            _redact_sensitive_values,
            redact_fields=config.redact_fields,
        ),
        structlog.processors.CallsiteParameterAdder(
            parameters=("pathname", "lineno", "func_name"),
            additional_ignores=("structlog", "logging"),
            key="caller",
        ),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.ExceptionPrettyPrinter(),
    ]


def _renderer_for(format: LogFormat) -> Any:
    if format is LogFormat.KEY_VALUE:
        return structlog.processors.KeyValueRenderer(
            key_order=_KEY_ORDER,
            sort_keys=False,
            drop_missing=True,
        )
    return structlog.processors.JSONRenderer(sort_keys=True, ensure_ascii=False)


def configure_logging(config: LogConfig | None = None) -> None:
    """Initialise logging based on the supplied configuration."""

    cfg = config or LogConfig()
    shared_processors = _shared_processors(cfg)
    renderer = _renderer_for(cfg.format)

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[*shared_processors, structlog.processors.filter_by_level],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logging.basicConfig(handlers=[handler], level=_coerce_log_level(cfg.level), force=True)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.filter_by_level,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def bind_global_context(**kwargs: Any) -> None:
    """Bind context that should appear on every log line going forward."""

    bind_contextvars(**kwargs)


def reset_global_context() -> None:
    """Clear previously bound global context."""

    clear_contextvars()


def get_logger(name: str = "bioetl") -> structlog.stdlib.BoundLogger:
    """Return a configured bound logger."""

    return structlog.get_logger(name)
