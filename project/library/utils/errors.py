"""Custom exceptions for the pipeline."""
from __future__ import annotations


class PipelineError(Exception):
    """Base class for pipeline errors."""


class SourceRequestError(PipelineError):
    """Raised when a source returns an unexpected response."""


class ValidationFailure(PipelineError):
    """Raised when validation fails for input or output data."""
