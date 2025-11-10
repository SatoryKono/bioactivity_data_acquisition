"""Utilities for configuring the project wide structured logger.

This module centralises the logging setup so that every service and
command line entry-point uses the exact same configuration.  The
implementation intentionally favours determinism and explicit context
binding so that logs can be consumed by observability pipelines as well
as by golden tests.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator, Mapping, MutableMapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Any, Final, cast

import structlog
from structlog.contextvars import (
    bind_contextvars,
    clear_contextvars,
    get_contextvars,
    unbind_contextvars,
)
from structlog.exceptions import DropEvent
from structlog.stdlib import BoundLogger

__all__ = [
    "LogFormat",
    "LogConfig",
    "DEFAULT_LOG_LEVEL",
    "MANDATORY_FIELDS",
    "configure_logging",
    "bind_global_context",
    "reset_global_context",
    "get_logger",
    "LoggerConfig",
    "UnifiedLogger",
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

_DEFAULT_LOGGER_NAME: Final[str] = "bioetl"
_LOG_METHOD_TO_LEVEL: Mapping[str, int] = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "warn": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "exception": logging.ERROR,
    "fatal": logging.CRITICAL,
}

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
    level_name = level.upper()
    mapping = _get_level_name_mapping()
    mapped_level = mapping.get(level_name)
    if isinstance(mapped_level, int):
        return mapped_level
    raise ValueError(f"Unsupported log level: {level}")


def _get_level_name_mapping() -> Mapping[str, int]:
    get_mapping = getattr(logging, "getLevelNamesMapping", None)
    if callable(get_mapping):
        mapping = get_mapping()
        if isinstance(mapping, Mapping):
            return cast(Mapping[str, int], mapping)
    # Fallback to standard level names if advanced mapping is unavailable.
    return {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }


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
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
        _ensure_mandatory_fields,
        structlog.processors.EventRenamer("message"),
        partial(
            _redact_sensitive_values,
            redact_fields=config.redact_fields,
        ),
        structlog.processors.CallsiteParameterAdder(
            parameters=(
                structlog.processors.CallsiteParameter.PATHNAME,
                structlog.processors.CallsiteParameter.LINENO,
                structlog.processors.CallsiteParameter.FUNC_NAME,
            ),
            additional_ignores=["structlog", "logging"],
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


def _safe_filter_by_level(
    logger: logging.Logger | None,
    method_name: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """Drop events that do not satisfy the active logging level."""

    effective_logger = logger or logging.getLogger(_DEFAULT_LOGGER_NAME)
    level = _LOG_METHOD_TO_LEVEL.get(method_name.lower(), logging.INFO)
    if effective_logger.isEnabledFor(level):
        return event_dict
    raise DropEvent


def configure_logging(
    config: LogConfig | None = None,
    *,
    additional_processors: Sequence[Any] | None = None,
) -> None:
    """Initialise logging based on the supplied configuration."""

    cfg = config or LogConfig()
    shared_processors = _shared_processors(cfg)
    if additional_processors:
        shared_processors = [*shared_processors, *additional_processors]
    renderer = _renderer_for(cfg.format)

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[*shared_processors, _safe_filter_by_level],
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
            _safe_filter_by_level,
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


def get_logger(name: str = "bioetl") -> BoundLogger:
    """Return a configured bound logger."""

    logger = structlog.get_logger(name)
    if isinstance(logger, BoundLogger):
        return logger
    if hasattr(logger, "bind") and callable(getattr(logger, "bind", None)):
        return cast(BoundLogger, logger)
    raise TypeError("structlog.get_logger returned unexpected logger type")


# ---------------------------------------------------------------------------
# Unified logger facade
# ---------------------------------------------------------------------------

# ``LoggerConfig`` maintains backwards compatibility with the documentation
# that refers to this alias while the implementation historically exposed
# ``LogConfig``.  Both names intentionally point to the exact same dataclass so
# that call sites can use either identifier without behavioural differences.
LoggerConfig = LogConfig


def _restore_previous_context(previous: dict[str, Any]) -> None:
    """Re-bind context values that existed before a scoped override."""

    if not previous:
        return
    bind_contextvars(**previous)


class UnifiedLogger:
    """Facade that exposes a minimal, documented logging API."""

    _default_logger_name = _DEFAULT_LOGGER_NAME

    @staticmethod
    def configure(
        config: LoggerConfig | None = None,
        *,
        additional_processors: Sequence[Any] | None = None,
    ) -> None:
        """Configure the underlying structured logger."""

        configure_logging(config, additional_processors=additional_processors)

    @staticmethod
    def get(name: str | None = None) -> BoundLogger:
        """Return a configured bound logger."""

        return get_logger(name or UnifiedLogger._default_logger_name)

    @staticmethod
    def bind(**context: Any) -> None:
        """Bind context that should be included with all subsequent log events."""

        bind_global_context(**context)

    @staticmethod
    def reset() -> None:
        """Reset all bound context variables."""

        reset_global_context()

    @staticmethod
    def scoped(**context: Any) -> AbstractContextManager[None]:
        """Return a context manager that temporarily overrides bound context."""

        from contextlib import contextmanager

        @contextmanager
        def _scope() -> Iterator[None]:
            existing = get_contextvars()
            previous = {key: existing[key] for key in context if key in existing}
            bind_contextvars(**context)
            try:
                yield None
            finally:
                unbind_contextvars(*context.keys())
                _restore_previous_context(previous)

        return _scope()

    @staticmethod
    def stage(stage: str, **context: Any) -> AbstractContextManager[None]:
        """Shortcut for temporarily binding the ``stage`` context field."""

        return UnifiedLogger.scoped(stage=stage, **context)
