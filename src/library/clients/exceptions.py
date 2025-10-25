"""DEPRECATED: Exceptions for API clients.

This module is deprecated and will be removed in a future version.
Use library.common.exceptions instead.
"""

import warnings

warnings.warn("library.clients.exceptions is deprecated. Use library.common.exceptions instead.", DeprecationWarning, stacklevel=2)

# Re-export from the new unified exceptions system for backward compatibility
from library.common.exceptions import ApiClientError as _ApiClientError
from library.common.exceptions import RateLimitError as _RateLimitError


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
