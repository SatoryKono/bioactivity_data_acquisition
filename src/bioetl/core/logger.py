"""Unified structured logging built on top of ``structlog``."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import re
from typing import Any, Mapping, MutableMapping, cast

import structlog

MANDATORY_CONTEXT_FIELDS: tuple[str, ...] = (
    "run_id",
    "stage",
    "actor",
    "source",
    "generated_at",
)

HTTP_CONTEXT_FIELDS: frozenset[str] = frozenset(
    {"endpoint", "attempt", "duration_ms", "params", "retry_after"}
)


def _ensure_aware_datetime(value: datetime | None) -> datetime:
    """Normalise ``generated_at`` values to timezone-aware UTC datetimes."""

    if value is None:
        return datetime.now(timezone.utc)

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def _serialise_datetime(value: datetime) -> str:
    """Serialise datetimes in ISO8601 with UTC normalisation."""

    return value.astimezone(timezone.utc).isoformat()


def _copy_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    """Return a shallow ``dict`` copy of an arbitrary mapping."""

    if isinstance(mapping, dict):
        return dict(mapping)
    return {key: value for key, value in mapping.items()}


@dataclass(slots=True)
class HttpRequestContext:
    """Context metadata specific to HTTP requests."""

    endpoint: str
    attempt: int
    duration_ms: float | None = None
    params: Mapping[str, Any] | None = None
    retry_after: float | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "endpoint": self.endpoint,
            "attempt": int(self.attempt),
        }
        if self.duration_ms is not None:
            payload["duration_ms"] = float(self.duration_ms)
        if self.params is not None:
            payload["params"] = _copy_mapping(self.params)
        if self.retry_after is not None:
            payload["retry_after"] = float(self.retry_after)
        return payload


@dataclass(slots=True)
class LogContext:
    """Structured log context persisted in a :class:`ContextVar`."""

    run_id: str
    stage: str
    actor: str
    source: str
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    extras: MutableMapping[str, Any] = field(default_factory=dict)
    http: dict[str, Any] | None = None

    def __post_init__(self) -> None:  # noqa: D401 - short hook
        self.run_id = str(self.run_id)
        self.stage = str(self.stage)
        self.actor = str(self.actor)
        self.source = str(self.source)
        self.generated_at = _ensure_aware_datetime(self.generated_at)
        self.extras = dict(self.extras)
        if self.http:
            self.http = {
                key: value for key, value in self.http.items() if key in HTTP_CONTEXT_FIELDS
            } or None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "run_id": self.run_id,
            "stage": self.stage,
            "actor": self.actor,
            "source": self.source,
            "generated_at": _serialise_datetime(self.generated_at),
        }
        if self.extras:
            payload.update(self.extras)
        if self.http:
            payload.update(self.http)
        return payload

    def evolve(self, **updates: Any) -> "LogContext":
        data: dict[str, Any] = {
            "run_id": self.run_id,
            "stage": self.stage,
            "actor": self.actor,
            "source": self.source,
            "generated_at": self.generated_at,
        }
        extras = dict(self.extras)
        http_context = dict(self.http or {})

        http_payload = updates.pop("http", None)
        if http_payload is not None:
            http_context.update(_coerce_http_context(http_payload))

        for key, value in updates.items():
            if key in MANDATORY_CONTEXT_FIELDS:
                if key == "generated_at":
                    data[key] = _ensure_aware_datetime(cast(datetime | None, value))
                else:
                    data[key] = str(value)
            elif key in HTTP_CONTEXT_FIELDS:
                if value is None:
                    http_context.pop(key, None)
                else:
                    http_context[key] = value
            else:
                if value is None:
                    extras.pop(key, None)
                else:
                    extras[key] = value

        return LogContext(
            run_id=data["run_id"],
            stage=data["stage"],
            actor=data["actor"],
            source=data["source"],
            generated_at=data["generated_at"],
            extras=extras,
            http=http_context or None,
        )


def _coerce_http_context(candidate: Any) -> dict[str, Any]:
    if candidate is None:
        return {}
    if isinstance(candidate, HttpRequestContext):
        return candidate.to_dict()
    if isinstance(candidate, Mapping):
        payload: dict[str, Any] = {}
        for key, value in candidate.items():
            if key not in HTTP_CONTEXT_FIELDS:
                continue
            if key == "params" and value is not None:
                payload[key] = _copy_mapping(cast(Mapping[str, Any], value))
            elif value is not None:
                payload[key] = value
        return payload
    raise TypeError("http context must be mapping or HttpRequestContext instance")


_log_context: ContextVar[LogContext | None] = ContextVar("log_context", default=None)


def _sync_structlog_context(context: LogContext | None) -> None:
    structlog.contextvars.clear_contextvars()
    if context is not None:
        structlog.contextvars.bind_contextvars(**context.to_dict())


def set_context(
    context: LogContext | None = None,
    *,
    replace: bool = False,
    http: Mapping[str, Any] | HttpRequestContext | None = None,
    **fields: Any,
) -> Token[LogContext | None]:
    """Bind structured context for subsequent log records."""

    base_context = None if replace else _log_context.get()

    if context is not None and fields:
        raise ValueError("context object and keyword fields are mutually exclusive")

    updates = dict(fields)
    if http is not None:
        updates["http"] = http

    if context is not None:
        new_context = context
    elif base_context is None:
        mandatory_without_generated_at = [
            field for field in MANDATORY_CONTEXT_FIELDS if field != "generated_at"
        ]
        missing = [
            field for field in mandatory_without_generated_at if field not in updates
        ]
        if missing:
            missing_labels = ", ".join(sorted(missing))
            raise ValueError(f"Missing required context fields: {missing_labels}")

        generated_at_value = updates.pop("generated_at", None)
        http_payload = _extract_http_updates(updates)
        extras = {
            key: value
            for key, value in updates.items()
            if key not in MANDATORY_CONTEXT_FIELDS and key not in HTTP_CONTEXT_FIELDS
        }
        new_context = LogContext(
            run_id=str(updates["run_id"]),
            stage=str(updates["stage"]),
            actor=str(updates["actor"]),
            source=str(updates["source"]),
            generated_at=_ensure_aware_datetime(cast(datetime | None, generated_at_value)),
            extras=extras,
            http=http_payload or None,
        )
    else:
        http_payload = _extract_http_updates(updates)
        if http_payload:
            updates["http"] = http_payload
        new_context = base_context.evolve(**updates)

    token = _log_context.set(new_context)
    _sync_structlog_context(new_context)
    return token


def _extract_http_updates(updates: MutableMapping[str, Any]) -> dict[str, Any]:
    http_payload: dict[str, Any] = {}
    raw_http = updates.pop("http", None)
    if raw_http is not None:
        http_payload.update(_coerce_http_context(raw_http))
    for key in tuple(updates.keys()):
        if key in HTTP_CONTEXT_FIELDS:
            value = updates.pop(key)
            if value is None:
                continue
            if key == "params" and isinstance(value, Mapping):
                http_payload[key] = _copy_mapping(value)
            else:
                http_payload[key] = value
    return http_payload


def reset_context(token: Token[LogContext | None]) -> None:
    """Restore the context that was active prior to ``set_context``."""

    _log_context.reset(token)
    _sync_structlog_context(_log_context.get())


def clear_context() -> None:
    """Remove any bound context from the current task."""

    _log_context.set(None)
    structlog.contextvars.clear_contextvars()


@contextmanager
def http_context(**fields: Any):
    """Temporarily augment the active context with HTTP metadata."""

    token = set_context(**fields)
    try:
        yield
    finally:
        reset_context(token)


@dataclass
class LoggerConfig:
    """Configuration for the structured logger."""

    level: str = "INFO"
    console_format: str = "text"  # text or json
    file_enabled: bool = True
    file_path: Path = Path("data/logs/app.log")
    file_format: str = "json"
    max_bytes: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 10
    telemetry_enabled: bool = False
    redact_secrets: bool = True


def security_processor(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Redact secret-like values from the structured event."""

    sensitive_keys = [
        "api_key",
        "token",
        "password",
        "secret",
        "authorization",
        "bearer",
        "auth",
        "credential",
        "access_token",
    ]

    for key in list(event_dict.keys()):
        if any(term in key.lower() for term in sensitive_keys):
            event_dict[key] = "***REDACTED***"

    return event_dict


class RedactSecretsFilter(logging.Filter):
    """Filter that redacts secrets in log records."""

    _patterns = [
        (
            re.compile(r"(?i)(token|api_key|password)\s*=\s*([^\s,}]+)", re.IGNORECASE),
            r"\1=***REDACTED***",
        ),
        (
            re.compile(r"(?i)(authorization)\s*:\s*([^\s,}]+)", re.IGNORECASE),
            r"\1: ***REDACTED***",
        ),
    ]

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401 - logging API
        if hasattr(record, "getMessage"):
            message = record.getMessage()
            for pattern, replacement in self._patterns:
                message = pattern.sub(replacement, message)
            record.msg = message
        return True


class SafeFormattingFilter(logging.Filter):
    """Protect against ``urllib3`` formatting edge-cases."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401 - logging API
        if "urllib3" in record.name and isinstance(record.msg, str):
            record.msg = f"urllib3: {record.msg}"
            record.args = ()

        if isinstance(record.msg, str):
            try:
                if record.args:
                    _ = record.msg % record.args
            except (TypeError, ValueError):
                if record.args:
                    record.args = tuple(str(arg) for arg in record.args)
        return True


def add_utc_timestamp(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Attach an event timestamp in UTC."""

    event_dict["timestamp"] = datetime.now(timezone.utc).isoformat()
    return event_dict


def add_context(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Inject the active :class:`LogContext` into the event payload."""

    context = _log_context.get()
    if context is not None:
        event_dict.update(context.to_dict())
    return event_dict


def setup_logger(
    mode: str = "development",
    run_id: str | None = None,
    config: LoggerConfig | None = None,
) -> None:
    """Configure ``structlog`` and standard logging backends."""

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("urllib3.util.retry").setLevel(logging.WARNING)

    logger_config = config or LoggerConfig()

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        add_utc_timestamp,
        add_context,
        security_processor,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.format_exc_info,
        structlog.processors.StackInfoRenderer(),
    ]

    if mode == "production":
        processors.append(structlog.processors.JSONRenderer())
        level = "INFO"
    elif mode == "testing":
        processors.append(structlog.processors.UnicodeDecoder())
        level = "WARNING"
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))
        level = "DEBUG"

    if config is not None:
        level = logger_config.level.upper()

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
        force=True,
    )

    root_logger = logging.getLogger()

    if logger_config.file_enabled:
        logger_config.file_path.parent.mkdir(parents=True, exist_ok=True)
        target_path = logger_config.file_path.resolve()
        has_file_handler = any(
            isinstance(handler, RotatingFileHandler)
            and hasattr(handler, "baseFilename")
            and Path(handler.baseFilename).resolve() == target_path
            for handler in root_logger.handlers
        )

        if not has_file_handler:
            file_handler = RotatingFileHandler(
                logger_config.file_path,
                maxBytes=logger_config.max_bytes,
                backupCount=logger_config.backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(logging.Formatter("%(message)s"))
            root_logger.addHandler(file_handler)

    for handler in root_logger.handlers:
        handler.addFilter(RedactSecretsFilter())
        handler.addFilter(SafeFormattingFilter())

    if run_id is not None:
        try:
            set_context(
                run_id=run_id,
                stage="bootstrap",
                actor="bootstrap",
                source="system",
            )
        except ValueError:
            # run-scoped context will be supplied explicitly by the caller
            pass


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structured logger bound to ``name``."""

    logger = structlog.get_logger(name)
    return cast(structlog.stdlib.BoundLogger, logger)


class UnifiedLogger:
    """Facade wrapping structlog utilities for the project."""

    _initialized = False
    _run_id: str | None = None

    @classmethod
    def setup(
        cls,
        mode: str = "development",
        run_id: str | None = None,
        config: LoggerConfig | None = None,
    ) -> None:
        cls._initialized = True
        cls._run_id = run_id
        setup_logger(mode=mode, run_id=run_id, config=config)

    @classmethod
    def get(cls, name: str) -> structlog.stdlib.BoundLogger:
        if not cls._initialized:
            cls.setup()
        return get_logger(name)

    @classmethod
    def set_context(cls, replace: bool = False, **kwargs: Any) -> None:
        set_context(replace=replace, **kwargs)

    @classmethod
    def reset_context(cls, token: Token[LogContext | None]) -> None:
        reset_context(token)

    @classmethod
    def clear_context(cls) -> None:
        clear_context()

    @classmethod
    @contextmanager
    def http_context(cls, **fields: Any):
        token = set_context(**fields)
        try:
            yield
        finally:
            reset_context(token)

    @classmethod
    def get_run_id(cls) -> str | None:
        return cls._run_id


def security_sensitive_message(message: str) -> str:
    """Utility retained for backwards compatibility."""

    return security_processor(None, "", {"message": message}).get("message", message)


__all__ = [
    "LoggerConfig",
    "LogContext",
    "HttpRequestContext",
    "UnifiedLogger",
    "add_context",
    "add_utc_timestamp",
    "clear_context",
    "get_logger",
    "http_context",
    "reset_context",
    "security_processor",
    "set_context",
]
