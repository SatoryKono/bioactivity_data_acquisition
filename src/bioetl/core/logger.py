"""Utilities for configuring the project wide structured logger.

This module centralises the logging setup so that every service and
command line entry-point uses the exact same configuration.  The
implementation intentionally favours determinism and explicit context
binding so that logs can be consumed by observability pipelines as well
as by golden tests.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator, MutableMapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Any

import structlog
from structlog.contextvars import (
    bind_contextvars,
    clear_contextvars,
    get_contextvars,
    unbind_contextvars,
)

from bioetl.config.models import LoggingConfig as PydanticLoggingConfig

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
    """User configurable logging parameters.

    This dataclass is kept for backward compatibility.
    For new code, prefer using PydanticLoggingConfig from bioetl.config.models.
    """

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


def _pydantic_config_to_dataclass(pydantic_config: PydanticLoggingConfig) -> LogConfig:
    """Convert Pydantic LoggingConfig to dataclass LogConfig."""
    format_value = LogFormat.JSON
    if isinstance(pydantic_config.format, str):
        try:
            format_value = LogFormat(pydantic_config.format)
        except ValueError:
            format_value = LogFormat.JSON
    return LogConfig(
        level=pydantic_config.level,
        format=format_value,
        redact_fields=tuple(pydantic_config.redact_fields),
    )


def configure_logging(
    config: LogConfig | PydanticLoggingConfig | None = None,
    *,
    additional_processors: Sequence[Any] | None = None,
) -> None:
    """Initialise logging based on the supplied configuration.

    Parameters
    ----------
    config:
        Either a LogConfig dataclass or PydanticLoggingConfig from models.
    additional_processors:
        Optional additional structlog processors.
    """
    if isinstance(config, PydanticLoggingConfig):
        cfg = _pydantic_config_to_dataclass(config)
    else:
        cfg = config or LogConfig()
    shared_processors = _shared_processors(cfg)
    if additional_processors:
        shared_processors = [*shared_processors, *additional_processors]
    renderer = _renderer_for(cfg.format)

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[*shared_processors, structlog.stdlib.filter_by_level],
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
            structlog.stdlib.filter_by_level,
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

    _default_logger_name = "bioetl"

    @staticmethod
    def configure(
        config: LoggerConfig | PydanticLoggingConfig | None = None,
        *,
        additional_processors: Sequence[Any] | None = None,
    ) -> None:
        """Configure the underlying structured logger.

        Parameters
        ----------
        config:
            Either a LoggerConfig dataclass or PydanticLoggingConfig from models.
        additional_processors:
            Optional additional structlog processors.
        """
        configure_logging(config, additional_processors=additional_processors)

    @staticmethod
    def get(name: str | None = None) -> structlog.stdlib.BoundLogger:
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
