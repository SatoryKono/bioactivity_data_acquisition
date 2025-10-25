"""Standardized exit codes for all ETL pipelines."""

from enum import IntEnum


class ExitCode(IntEnum):
    """Standardized exit codes for all ETL pipelines.

    These codes are used consistently across all pipeline commands
    to provide uniform error reporting and integration with CI/CD systems.
    """

    OK = 0
    """Successful execution."""

    VALIDATION_ERROR = 1
    """Input validation failed (invalid data, missing required fields, etc.)."""

    HTTP_ERROR = 2
    """HTTP/API communication failed (timeouts, rate limits, server errors)."""

    QC_ERROR = 3
    """Quality control checks failed (data quality below thresholds)."""

    IO_ERROR = 4
    """File I/O operations failed (read/write errors, permissions, etc.)."""

    CONFIG_ERROR = 5
    """Configuration error (invalid YAML, missing required settings, etc.)."""
