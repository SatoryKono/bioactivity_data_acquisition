"""UnifiedLogger: structured logging with secret redaction, UTC timestamps, ContextVar."""

import logging
import re
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from logging.handlers import RotatingFileHandler

import structlog

# ContextVar для контекста логирования
_log_context: ContextVar[dict[str, Any] | None] = ContextVar("log_context", default=None)


@dataclass
class LoggerConfig:
    """Configuration for UnifiedLogger."""

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
    """Remove secrets from event_dict."""
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
        if any(s in key.lower() for s in sensitive_keys):
            event_dict[key] = "***REDACTED***"

    return event_dict


class RedactSecretsFilter(logging.Filter):
    """Filter that redacts secrets in log records."""

    def __init__(self) -> None:
        super().__init__()
        self.patterns = [
            (
                re.compile(r"(?i)(token|api_key|password)\s*=\s*([^\s,}]+)", re.IGNORECASE),
                r"\1=***REDACTED***",
            ),
            (
                re.compile(r"(?i)(authorization)\s*:\s*([^\s,}]+)", re.IGNORECASE),
                r"\1: ***REDACTED***",
            ),
        ]

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter and redact secrets."""
        if hasattr(record, "getMessage"):
            message = record.getMessage()
            for pattern, replacement in self.patterns:
                message = pattern.sub(replacement, message)
            record.msg = message
        return True


class SafeFormattingFilter(logging.Filter):
    """Filter that protects against formatting errors in urllib3."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter and protect formatting."""
        if "urllib3" in record.name:
            if hasattr(record, "msg") and isinstance(record.msg, str):
                record.msg = f"urllib3: {record.msg}"
                record.args = ()

        if hasattr(record, "msg") and isinstance(record.msg, str):
            try:
                if hasattr(record, "args") and record.args:
                    _ = record.msg % record.args
            except (TypeError, ValueError):
                if hasattr(record, "args") and record.args:
                    safe_args = [str(arg) for arg in record.args]
                    record.args = tuple(safe_args)

        return True


def add_utc_timestamp(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Add UTC timestamp to event dict."""
    event_dict["timestamp"] = datetime.now(timezone.utc).isoformat()
    return event_dict


def add_context(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Add context from ContextVar to event dict."""
    context = _log_context.get()
    if context:
        event_dict.update(context)
    return event_dict


def setup_logger(
    mode: str = "development",
    run_id: str | None = None,
    config: LoggerConfig | None = None,
) -> None:
    """
    Setup UnifiedLogger with specified mode.

    Args:
        mode: 'development', 'production', or 'testing'
        run_id: Optional run ID for context
    """
    # Suppress verbose urllib3 logs first, before any logging setup
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("urllib3.util.retry").setLevel(logging.WARNING)

    # Set context with run_id if provided
    if run_id:
        set_context({"run_id": run_id})

    # Load configuration (with defaults) and configure structlog processors
    logger_config = config or LoggerConfig()

    processors: list[Callable[..., Any]] = [
        structlog.contextvars.merge_contextvars,
        add_utc_timestamp,
        add_context,
        security_processor,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.format_exc_info,
        structlog.processors.StackInfoRenderer(),
    ]

    # Configure renderer based on mode
    if mode == "production":
        # JSON format for production
        processors.append(structlog.processors.JSONRenderer())
        level = "INFO"
    elif mode == "testing":
        # Minimal output for testing
        level = "WARNING"
        processors.append(structlog.processors.UnicodeDecoder())
    else:  # development
        # Human-readable format
        level = "DEBUG"
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    if config is not None:
        level = logger_config.level.upper()

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=None,
        level=getattr(logging, level.upper(), logging.INFO),
        force=True,
    )

    # Add filters to root logger
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


def set_context(context: dict[str, Any]) -> None:
    """Set logging context."""
    _log_context.set(context)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get structured logger instance."""
    return structlog.get_logger(name)


class UnifiedLogger:
    """Unified logger interface."""

    _initialized = False
    _run_id: str | None = None

    @classmethod
    def setup(
        cls,
        mode: str = "development",
        run_id: str | None = None,
        config: LoggerConfig | None = None,
    ) -> None:
        """Setup the unified logger."""
        cls._initialized = True
        cls._run_id = run_id
        setup_logger(mode=mode, run_id=run_id, config=config)

    @classmethod
    def get(cls, name: str) -> structlog.stdlib.BoundLogger:
        """Get logger instance."""
        if not cls._initialized:
            cls.setup()
        return get_logger(name)

    @classmethod
    def set_context(cls, **kwargs: Any) -> None:
        """Set context for logging."""
        set_context(kwargs)

    @classmethod
    def get_run_id(cls) -> str | None:
        """Get current run ID."""
        return cls._run_id

