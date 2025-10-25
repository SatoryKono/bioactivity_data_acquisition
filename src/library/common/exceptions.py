"""Unified exception hierarchy for the bioactivity data acquisition system.

This module provides a comprehensive set of exceptions organized by domain
and severity, with proper inheritance hierarchy and context information.
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from enum import Enum
from typing import Any


class ErrorSeverity(Enum):
    """Error severity levels."""

    LOW = "low"  # Non-critical, recoverable
    MEDIUM = "medium"  # Important, may affect data quality
    HIGH = "high"  # Critical, affects pipeline execution
    CRITICAL = "critical"  # Fatal, stops pipeline


class ErrorDomain(Enum):
    """Error domains for categorization."""

    CONFIG = "config"  # Configuration errors
    NETWORK = "network"  # Network/HTTP errors
    DATA = "data"  # Data processing errors
    VALIDATION = "validation"  # Schema validation errors
    NORMALIZATION = "normalization"  # Data normalization errors
    CACHE = "cache"  # Caching errors
    RATE_LIMIT = "rate_limit"  # Rate limiting errors
    XML = "xml"  # XML parsing errors
    UNKNOWN = "unknown"  # Unclassified errors


@dataclass
class ErrorContext:
    """Context information for errors."""

    domain: ErrorDomain
    severity: ErrorSeverity
    component: str | None = None
    operation: str | None = None
    details: dict[str, Any] | None = None
    traceback: str | None = None


class BioactivityError(Exception):
    """Base exception for all bioactivity data acquisition errors."""

    def __init__(self, message: str, *, context: ErrorContext | None = None, cause: Exception | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.context = context or ErrorContext(domain=ErrorDomain.UNKNOWN, severity=ErrorSeverity.MEDIUM)
        self.cause = cause

        # Capture traceback if not provided
        if self.context.traceback is None:
            self.context.traceback = traceback.format_exc()

    def __str__(self) -> str:
        base_msg = self.message
        if self.context.component:
            base_msg = f"[{self.context.component}] {base_msg}"
        if self.context.operation:
            base_msg = f"{base_msg} (operation: {self.context.operation})"
        return base_msg

    def to_dict(self) -> dict[str, Any]:
        """Convert error to dictionary for logging/serialization."""
        return {
            "message": self.message,
            "domain": self.context.domain.value,
            "severity": self.context.severity.value,
            "component": self.context.component,
            "operation": self.context.operation,
            "details": self.context.details,
            "cause": str(self.cause) if self.cause else None,
            "traceback": self.context.traceback,
        }


# Configuration Errors
class ConfigError(BioactivityError):
    """Raised when configuration files are missing or invalid."""

    def __init__(self, message: str, *, config_file: str | None = None, config_section: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(domain=ErrorDomain.CONFIG, severity=ErrorSeverity.HIGH, component="config", details={"config_file": config_file, "config_section": config_section})
        super().__init__(message, cause=cause)


class ConfigLoadError(ConfigError):
    """Raised when configuration loading fails."""

    pass


# Network/HTTP Errors
class NetworkError(BioactivityError):
    """Base class for network-related errors."""

    def __init__(self, message: str, *, url: str | None = None, status_code: int | None = None, component: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(domain=ErrorDomain.NETWORK, severity=ErrorSeverity.MEDIUM, component=component, details={"url": url, "status_code": status_code})
        super().__init__(message, cause=cause)


class ApiClientError(NetworkError):
    """Generic API client error."""

    def __init__(self, message: str, *, url: str | None = None, status_code: int | None = None, api_name: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(
            domain=ErrorDomain.NETWORK, severity=ErrorSeverity.MEDIUM, component=api_name or "api_client", details={"url": url, "status_code": status_code, "api_name": api_name}
        )
        super().__init__(message, cause=cause)


class RateLimitError(ApiClientError):
    """Raised when rate limit is exceeded."""

    def __init__(self, message: str, *, url: str | None = None, retry_after: float | None = None, api_name: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(
            domain=ErrorDomain.RATE_LIMIT,
            severity=ErrorSeverity.MEDIUM,
            component=api_name or "rate_limiter",
            details={"url": url, "retry_after": retry_after, "api_name": api_name},
        )
        super().__init__(message, cause=cause)


class TimeoutError(NetworkError):
    """Raised when request times out."""

    def __init__(self, message: str, *, url: str | None = None, timeout: float | None = None, component: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(domain=ErrorDomain.NETWORK, severity=ErrorSeverity.MEDIUM, component=component, details={"url": url, "timeout": timeout})
        super().__init__(message, cause=cause)


# Data Processing Errors
class DataError(BioactivityError):
    """Base class for data processing errors."""

    def __init__(self, message: str, *, data_source: str | None = None, record_id: str | None = None, component: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(domain=ErrorDomain.DATA, severity=ErrorSeverity.MEDIUM, component=component, details={"data_source": data_source, "record_id": record_id})
        super().__init__(message, cause=cause)


class ExtractionError(DataError):
    """Raised when data extraction fails."""

    def __init__(self, message: str, *, data_source: str | None = None, record_id: str | None = None, api_name: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(
            domain=ErrorDomain.DATA,
            severity=ErrorSeverity.HIGH,
            component=api_name or "extractor",
            operation="extract",
            details={"data_source": data_source, "record_id": record_id, "api_name": api_name},
        )
        super().__init__(message, cause=cause)


class TransformationError(DataError):
    """Raised when data transformation fails."""

    def __init__(self, message: str, *, data_source: str | None = None, record_id: str | None = None, transform_type: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(
            domain=ErrorDomain.DATA,
            severity=ErrorSeverity.MEDIUM,
            component="transformer",
            operation="transform",
            details={"data_source": data_source, "record_id": record_id, "transform_type": transform_type},
        )
        super().__init__(message, cause=cause)


# Validation Errors
class ValidationError(BioactivityError):
    """Raised when data validation fails."""

    def __init__(
        self,
        message: str,
        *,
        schema_name: str | None = None,
        field_name: str | None = None,
        record_id: str | None = None,
        validation_errors: list[str] | None = None,
        cause: Exception | None = None,
    ) -> None:
        context = ErrorContext(
            domain=ErrorDomain.VALIDATION,
            severity=ErrorSeverity.HIGH,
            component="validator",
            operation="validate",
            details={"schema_name": schema_name, "field_name": field_name, "record_id": record_id, "validation_errors": validation_errors},
        )
        super().__init__(message, cause=cause)


class SchemaValidationError(ValidationError):
    """Raised when Pandera schema validation fails."""

    pass


class DocumentValidationError(ValidationError):
    """Raised when document validation fails."""

    pass


class TestitemValidationError(ValidationError):
    """Raised when testitem validation fails."""

    pass


# Normalization Errors
class NormalizationError(BioactivityError):
    """Raised when data normalization fails."""

    def __init__(self, message: str, *, field_name: str | None = None, value: Any = None, normalizer_name: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(
            domain=ErrorDomain.NORMALIZATION,
            severity=ErrorSeverity.LOW,
            component="normalizer",
            operation="normalize",
            details={"field_name": field_name, "value": str(value) if value is not None else None, "normalizer_name": normalizer_name},
        )
        super().__init__(message, cause=cause)


# Cache Errors
class CacheError(BioactivityError):
    """Raised when caching operations fail."""

    def __init__(self, message: str, *, cache_key: str | None = None, cache_type: str | None = None, operation: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(
            domain=ErrorDomain.CACHE, severity=ErrorSeverity.LOW, component="cache", operation=operation, details={"cache_key": cache_key, "cache_type": cache_type}
        )
        super().__init__(message, cause=cause)


# XML Errors
class XMLParseError(BioactivityError):
    """Raised when XML parsing fails."""

    def __init__(self, message: str, *, xml_source: str | None = None, xpath: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(domain=ErrorDomain.XML, severity=ErrorSeverity.MEDIUM, component="xml_parser", operation="parse", details={"xml_source": xml_source, "xpath": xpath})
        super().__init__(message, cause=cause)


class XMLValidationError(XMLParseError):
    """Raised when XML validation fails."""

    def __init__(self, message: str, *, xml_source: str | None = None, schema_type: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(
            domain=ErrorDomain.XML, severity=ErrorSeverity.MEDIUM, component="xml_validator", operation="validate", details={"xml_source": xml_source, "schema_type": schema_type}
        )
        super().__init__(message, cause=cause)


class XPathError(XMLParseError):
    """Raised when XPath execution fails."""

    def __init__(self, message: str, *, xpath: str | None = None, xml_source: str | None = None, cause: Exception | None = None) -> None:
        context = ErrorContext(
            domain=ErrorDomain.XML, severity=ErrorSeverity.MEDIUM, component="xpath_processor", operation="xpath", details={"xpath": xpath, "xml_source": xml_source}
        )
        super().__init__(message, cause=cause)


# Utility functions
def create_error_context(domain: ErrorDomain, severity: ErrorSeverity, component: str | None = None, operation: str | None = None, **details: Any) -> ErrorContext:
    """Create error context with given parameters."""
    return ErrorContext(domain=domain, severity=severity, component=component, operation=operation, details=details if details else None)


def wrap_exception(
    exc: Exception,
    message: str | None = None,
    *,
    domain: ErrorDomain = ErrorDomain.UNKNOWN,
    severity: ErrorSeverity = ErrorSeverity.MEDIUM,
    component: str | None = None,
    operation: str | None = None,
    **details: Any,
) -> BioactivityError:
    """Wrap a generic exception in a BioactivityError."""
    if isinstance(exc, BioactivityError):
        return exc

    context = create_error_context(domain=domain, severity=severity, component=component, operation=operation, **details)

    return BioactivityError(message or str(exc), context=context, cause=exc)


# Legacy compatibility aliases
# These maintain backward compatibility with existing code
ConfigLoadError = ConfigLoadError  # Already defined above
ExtractionError = ExtractionError  # Already defined above
ValidationError = ValidationError  # Already defined above
NormalizationError = NormalizationError  # Already defined above
XMLParseError = XMLParseError  # Already defined above
XMLValidationError = XMLValidationError  # Already defined above
XPathError = XPathError  # Already defined above
