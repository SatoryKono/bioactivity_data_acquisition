"""Custom exceptions used across the publications ETL project."""
from __future__ import annotations

from typing import Optional

from requests import Response
from requests.exceptions import RequestException


class ProjectError(Exception):
    """Base class for all custom exceptions."""


class ConfigError(ProjectError):
    """Raised when configuration is invalid or missing."""


class ValidationError(ProjectError):
    """Raised when data fails validation checks."""


class ClientError(ProjectError):
    """Raised when a client returns a non-recoverable error."""


class RetryableHTTPError(RequestException):
    """Exception used to trigger retry/backoff with optional Retry-After awareness."""

    def __init__(self, message: str, response: Optional[Response] = None, wait_time: Optional[float] = None):
        super().__init__(message)
        self.response = response
        self.wait_time = wait_time


class PipelineExecutionError(ProjectError):
    """Raised when the pipeline execution encounters an unrecoverable issue."""


class SchemaMismatchError(ValidationError):
    """Raised when the Pandera schema validation fails."""

