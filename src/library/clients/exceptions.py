"""DEPRECATED: Exceptions for API clients.

This module is deprecated and will be removed in a future version.
Use library.common.exceptions instead.
"""

import warnings

from library.common.exceptions import ApiClientError as _ApiClientError
from library.common.exceptions import RateLimitError as _RateLimitError
from library.common.exceptions import TimeoutError as _TimeoutError
from library.common.exceptions import SchemaValidationError as _SchemaValidationError
from library.common.exceptions import NetworkError as _NetworkError
from library.common.exceptions import DataError as _DataError

warnings.warn("library.clients.exceptions is deprecated. Use library.common.exceptions instead.", DeprecationWarning, stacklevel=2)

# Re-export from the new unified exceptions system for backward compatibility


# Legacy compatibility classes
class ApiClientError(_ApiClientError):
    """Legacy compatibility wrapper for ApiClientError."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        warnings.warn("ApiClientError from library.clients.exceptions is deprecated. Use library.common.exceptions.ApiClientError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message, url=None, status_code=status_code, api_name=None)


class RateLimitError(_RateLimitError):
    """Legacy compatibility wrapper for RateLimitError."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        warnings.warn("RateLimitError from library.clients.exceptions is deprecated. Use library.common.exceptions.RateLimitError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message, url=None, api_name=None)


class APIClientError(_ApiClientError):
    """Legacy compatibility wrapper for APIClientError."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        warnings.warn("APIClientError from library.clients.exceptions is deprecated. Use library.common.exceptions.ApiClientError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message, url=None, status_code=status_code, api_name=None)


class APIConnectionError(_NetworkError):
    """Legacy compatibility wrapper for APIConnectionError."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        warnings.warn("APIConnectionError from library.clients.exceptions is deprecated. Use library.common.exceptions.NetworkError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message, url=None, status_code=status_code)


class APIRateLimitError(_RateLimitError):
    """Legacy compatibility wrapper for APIRateLimitError."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        warnings.warn("APIRateLimitError from library.clients.exceptions is deprecated. Use library.common.exceptions.RateLimitError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message, url=None, api_name=None)


class APITimeoutError(_TimeoutError):
    """Legacy compatibility wrapper for APITimeoutError."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        warnings.warn("APITimeoutError from library.clients.exceptions is deprecated. Use library.common.exceptions.TimeoutError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message, url=None)


class ResourceNotFoundError(_DataError):
    """Legacy compatibility wrapper for ResourceNotFoundError."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        warnings.warn("ResourceNotFoundError from library.clients.exceptions is deprecated. Use library.common.exceptions.DataError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message)


class SchemaValidationError(_SchemaValidationError):
    """Legacy compatibility wrapper for SchemaValidationError."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        warnings.warn("SchemaValidationError from library.clients.exceptions is deprecated. Use library.common.exceptions.SchemaValidationError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message)
