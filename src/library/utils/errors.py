"""Custom exception hierarchy for the publications ETL pipeline."""

from __future__ import annotations


class ConfigError(RuntimeError):
    """Raised when configuration files are missing or invalid."""


class ExtractionError(RuntimeError):
    """Raised when upstream APIs return errors or malformed payloads."""


class ValidationError(RuntimeError):
    """Raised when data does not satisfy the expected schema."""


__all__ = ["ConfigError", "ExtractionError", "ValidationError"]
