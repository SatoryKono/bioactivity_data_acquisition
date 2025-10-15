"""Exceptions for API clients."""


class ApiClientError(RuntimeError):
    """Generic client error."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class RateLimitError(ApiClientError):
    """Raised when the rate limiter rejects the request."""
