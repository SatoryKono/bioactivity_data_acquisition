"""Logging helpers for the ETL pipeline."""

from __future__ import annotations

import logging
from typing import Any, cast

import structlog
from structlog.stdlib import BoundLogger


def _redact_secrets_processor(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Remove sensitive information from log entries."""
    # List of keys that might contain secrets
    sensitive_keys = [
        "authorization",
        "api_key",
        "token",
        "password",
        "secret",
        "key",
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


def configure_logging(level: str = "INFO") -> BoundLogger:
    """Configure structlog for structured logging."""

    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            _redact_secrets_processor,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )
    logger = structlog.get_logger()
    return cast(BoundLogger, logger)


def bind_stage(logger: BoundLogger, stage: str, **extra: Any) -> BoundLogger:
    """Attach contextual metadata to the logger."""

    return logger.bind(stage=stage, **extra)


__all__ = ["bind_stage", "configure_logging"]
