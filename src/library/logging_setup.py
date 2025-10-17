"""Unified logging configuration for the bioactivity ETL pipeline."""

from __future__ import annotations

import logging
import logging.config
import re
import uuid
from contextvars import ContextVar
from pathlib import Path
from typing import Any

import structlog
import yaml
from structlog.stdlib import BoundLogger

# Context variables for structured logging
run_id_var: ContextVar[str | None] = ContextVar("run_id", default=None)
stage_var: ContextVar[str | None] = ContextVar("stage", default=None)

_LOGGING_CONFIGURED = False


class RedactSecretsFilter(logging.Filter):
    """Filter to redact sensitive information from log records."""
    
    def __init__(self, name: str = "") -> None:
        super().__init__(name)
        self.sensitive_patterns = [
            (re.compile(r'(?i)(token|api_key|password|secret|key)\s*=\s*([^\s,}]+)'), r'\1=[REDACTED]'),
            (re.compile(r'(?i)(authorization|bearer)\s*:\s*([^\s,}]+)'), r'\1: [REDACTED]'),
            (re.compile(r'(?i)(password|pwd)\s*=\s*([^\s,}]+)'), r'\1=[REDACTED]'),
        ]
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Filter and redact sensitive information from log record."""
        if hasattr(record, 'getMessage'):
            message = record.getMessage()
            for pattern, replacement in self.sensitive_patterns:
                message = pattern.sub(replacement, message)
            record.msg = message
        return True


class AddContextFilter(logging.Filter):
    """Filter to add context variables to log records."""
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add context variables to log record."""
        record.run_id = run_id_var.get() or "unknown"
        record.stage = stage_var.get() or "unknown"
        
        # Add trace_id from OpenTelemetry if available
        try:
            from library.telemetry import get_current_trace_id
            record.trace_id = get_current_trace_id() or "unknown"
        except ImportError:
            record.trace_id = "unknown"
        
        return True


def _redact_secrets_processor(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Remove sensitive information from structlog event dictionary."""
    sensitive_keys = [
        "authorization", "api_key", "token", "password", "secret", "key",
        "bearer", "auth", "credential", "access_token", "refresh_token"
    ]
    
    def redact_dict(d: dict[str, Any]) -> dict[str, Any]:
        """Recursively redact sensitive values in a dictionary."""
        result = {}
        for key, value in d.items():
            if isinstance(value, dict):
                result[key] = redact_dict(value)
            elif isinstance(value, str) and any(sensitive in key.lower() for sensitive in sensitive_keys):
                result[key] = "[REDACTED]"
            else:
                result[key] = value
        return result
    
    # Redact headers if present
    if "headers" in event_dict:
        event_dict["headers"] = redact_dict(event_dict["headers"])
    
    # Redact any other sensitive fields
    event_dict = redact_dict(event_dict)
    
    return event_dict


def _add_context_processor(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Add context variables to structlog event dictionary."""
    event_dict["run_id"] = run_id_var.get() or "unknown"
    event_dict["stage"] = stage_var.get() or "unknown"
    
    # Add trace_id from OpenTelemetry if available
    try:
        from library.telemetry import get_current_trace_id
        event_dict["trace_id"] = get_current_trace_id() or "unknown"
    except ImportError:
        event_dict["trace_id"] = "unknown"
    
    return event_dict


def set_run_context(run_id: str | None = None, stage: str | None = None) -> None:
    """Set context variables for structured logging."""
    if run_id is not None:
        run_id_var.set(run_id)
    if stage is not None:
        stage_var.set(stage)


def get_run_context() -> dict[str, str | None]:
    """Get current context variables."""
    return {
        "run_id": run_id_var.get(),
        "stage": stage_var.get(),
    }


def generate_run_id() -> str:
    """Generate a unique run ID for the current pipeline execution."""
    return str(uuid.uuid4())[:8]


def configure_logging(
    level: str = "INFO",
    config_path: Path | None = None,
    file_enabled: bool = True,
    console_format: str = "text",
    log_file: Path | None = None,
) -> BoundLogger:
    """Configure structured logging with file and console handlers.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        config_path: Path to logging configuration YAML file
        file_enabled: Whether to enable file logging
        console_format: Console format (text or json)
        log_file: Custom log file path
        
    Returns:
        Configured structlog logger
    """
    global _LOGGING_CONFIGURED
    
    if _LOGGING_CONFIGURED:
        return structlog.get_logger()
    
    # Create logs directory if file logging is enabled
    if file_enabled:
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
    
    # Load configuration from YAML if provided
    if config_path and config_path.exists():
        with config_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        # Override configuration based on parameters
        if not file_enabled:
            # Remove file handlers
            for logger_config in config.get("loggers", {}).values():
                if "handlers" in logger_config:
                    logger_config["handlers"] = [h for h in logger_config["handlers"] if h != "file"]
            if "root" in config and "handlers" in config["root"]:
                config["root"]["handlers"] = [h for h in config["root"]["handlers"] if h != "file"]
        
        if log_file:
            # Update file handler path
            if "handlers" in config and "file" in config["handlers"]:
                config["handlers"]["file"]["filename"] = str(log_file)
        
        # Apply configuration
        logging.config.dictConfig(config)
    else:
        # Fallback to programmatic configuration
        _configure_programmatic_logging(level, file_enabled, console_format, log_file)
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            _add_context_processor,
            _redact_secrets_processor,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer() if console_format == "json" else structlog.dev.ConsoleRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    _LOGGING_CONFIGURED = True
    return structlog.get_logger()


def _configure_programmatic_logging(
    level: str,
    file_enabled: bool,
    console_format: str,
    log_file: Path | None,
) -> None:
    """Configure logging programmatically without YAML config."""
    # Create formatters
    console_formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    json_formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s run_id:%(run_id)s stage:%(stage)s trace_id:%(trace_id)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    
    # Create handlers
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level.upper()))
    console_handler.setFormatter(console_formatter)
    console_handler.addFilter(RedactSecretsFilter())
    console_handler.addFilter(AddContextFilter())
    
    handlers = [console_handler]
    
    if file_enabled:
        file_path = log_file or Path("logs/app.log")
        file_handler = logging.handlers.RotatingFileHandler(
            filename=file_path,
            maxBytes=10485760,  # 10MB
            backupCount=10,
            encoding="utf-8"
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(json_formatter)
        file_handler.addFilter(RedactSecretsFilter())
        file_handler.addFilter(AddContextFilter())
        handlers.append(file_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        handlers=handlers,
        format="%(message)s"  # structlog will handle formatting
    )


def bind_stage(logger: BoundLogger, stage: str, **extra: Any) -> BoundLogger:
    """Attach contextual metadata to the logger."""
    set_run_context(stage=stage)
    return logger.bind(stage=stage, **extra)


def cleanup_old_logs(older_than_days: int = 14, logs_dir: Path | None = None) -> None:
    """Clean up log files older than specified days.
    
    Args:
        older_than_days: Remove logs older than this many days
        logs_dir: Directory to clean (defaults to logs/)
    """
    import time
    from datetime import datetime, timedelta
    
    if logs_dir is None:
        logs_dir = Path("logs")
    
    if not logs_dir.exists():
        return
    
    cutoff_time = time.time() - (older_than_days * 24 * 60 * 60)
    
    for log_file in logs_dir.rglob("*.log*"):
        if log_file.stat().st_mtime < cutoff_time:
            try:
                log_file.unlink()
                print(f"Removed old log file: {log_file}")
            except OSError as e:
                print(f"Failed to remove {log_file}: {e}")


__all__ = [
    "configure_logging",
    "set_run_context",
    "get_run_context",
    "generate_run_id",
    "bind_stage",
    "cleanup_old_logs",
    "RedactSecretsFilter",
    "AddContextFilter",
]
