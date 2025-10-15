"""Custom exceptions for the pipeline."""
from __future__ import annotations


class PipelineError(Exception):
    """Base class for pipeline errors."""


class ConfigError(RuntimeError):
    """Raised when configuration files are missing or invalid."""


class ExtractionError(RuntimeError):
    """Raised when upstream APIs return errors or malformed payloads."""


class SourceRequestError(PipelineError):
    """Raised when a source returns an unexpected response."""


class ValidationFailureError(PipelineError):
    """Raised when validation fails for input or output data."""


class ValidationError(RuntimeError):
    """Raised when data does not satisfy the expected schema."""
