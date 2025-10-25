"""Configuration package."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

# NOTE: This package shadows the library.config module (config.py).
# To avoid circular imports, we copy the most commonly used classes here.
# For full config functionality, import directly from library.config module.


class RetrySettings(BaseModel):
    """Retry configuration for HTTP clients."""

    model_config = ConfigDict(populate_by_name=True)

    total: int = Field(default=5, ge=1, alias="max_tries")
    backoff_multiplier: float = Field(default=1.0, gt=0)
    max_delay: float = Field(default=60.0, gt=0)

    @property
    def max_tries(self) -> int:
        """Backward-compatible accessor for legacy code paths."""
        return self.total


class RateLimitSettings(BaseModel):
    """Rate limiting configuration for API clients."""

    max_calls: int = Field(gt=0)
    period: float = Field(gt=0)
    burst: int | None = Field(default=None, ge=1, description="Burst capacity for token bucket limiter")


class APIClientConfig(BaseModel):
    """Configuration for a single HTTP API client."""

    name: str
    base_url: HttpUrl
    endpoint: str = ""
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    pagination_param: str | None = None
    page_size_param: str | None = None
    page_size: int | None = Field(default=None, gt=0)
    max_pages: int | None = Field(default=None, gt=0)
    timeout: float = Field(default=30.0, gt=0)
    timeout_connect: float | None = Field(default=None, gt=0, description="Connection timeout in seconds")
    timeout_read: float | None = Field(default=None, gt=0, description="Read timeout in seconds")
    retries: RetrySettings = Field(default_factory=RetrySettings)
    rate_limit: RateLimitSettings | None = None
    health_endpoint: str | None = Field(default=None, description="Custom health check endpoint. If None, uses base_url for health checks")
    mailto: str | None = Field(default=None, description="Email for polite pool access (e.g., Crossref API)")
    rps: float = Field(default=5.0, gt=0, description="Requests per second for rate limiting")
    burst: int = Field(default=10, gt=0, description="Burst capacity for rate limiting")

    model_config = ConfigDict(frozen=True)

    @property
    def resolved_base_url(self) -> str:
        base = str(self.base_url).rstrip("/")
        if not self.endpoint:
            return base
        endpoint = self.endpoint.lstrip("/")
        return f"{base}/{endpoint}"

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Client name must not be empty")
        return value


__all__ = [
    "APIClientConfig",
    "RetrySettings",
    "RateLimitSettings",
]
