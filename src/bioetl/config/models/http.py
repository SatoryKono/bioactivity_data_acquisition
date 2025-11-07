"""HTTP client configuration models."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, PositiveFloat, PositiveInt

StatusCode = Annotated[int, Field(ge=100, le=599)]


class RetryConfig(BaseModel):
    """Retry policy for HTTP clients."""

    model_config = ConfigDict(extra="forbid")

    total: Annotated[int, Field(ge=0)] = Field(
        default=5,
        description="Total number of retry attempts (excluding the first call).",
    )
    backoff_multiplier: PositiveFloat = Field(
        default=2.0,
        description="Multiplier applied between retry attempts for exponential backoff.",
    )
    backoff_max: PositiveFloat = Field(
        default=60.0,
        description="Maximum delay in seconds between retry attempts.",
    )
    statuses: tuple[StatusCode, ...] = Field(
        default=(408, 429, 500, 502, 503, 504),
        description="HTTP status codes that should trigger a retry.",
    )


class RateLimitConfig(BaseModel):
    """Leaky-bucket style client-side rate limiting."""

    model_config = ConfigDict(extra="forbid")

    max_calls: PositiveInt = Field(
        default=10,
        description="Maximum number of calls allowed within the configured period.",
    )
    period: PositiveFloat = Field(
        default=1.0,
        description="Time window in seconds for the rate limit.",
    )


class CircuitBreakerConfig(BaseModel):
    """Circuit breaker configuration for protecting against cascading failures."""

    model_config = ConfigDict(extra="forbid")

    failure_threshold: PositiveInt = Field(
        default=5,
        description="Number of consecutive failures before the circuit breaker opens.",
    )
    timeout: PositiveFloat = Field(
        default=60.0,
        description="Time in seconds the circuit breaker will stay open before transitioning to half-open.",
    )
    half_open_max_calls: PositiveInt = Field(
        default=1,
        description="Maximum number of calls allowed in half-open state before transitioning back to closed or open.",
    )


class HTTPClientConfig(BaseModel):
    """Configuration for a single logical HTTP client."""

    model_config = ConfigDict(extra="forbid")

    timeout_sec: PositiveFloat = Field(
        default=60.0, description="Total request timeout in seconds."
    )
    connect_timeout_sec: PositiveFloat = Field(
        default=15.0,
        description="Connection timeout in seconds.",
    )
    read_timeout_sec: PositiveFloat = Field(
        default=60.0,
        description="Socket read timeout in seconds.",
    )
    retries: RetryConfig = Field(default_factory=RetryConfig)
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    rate_limit_jitter: bool = Field(
        default=True,
        description="Whether to add jitter to rate limited calls to avoid thundering herds.",
    )
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
    headers: Mapping[str, str] = Field(
        default_factory=lambda: {
            "User-Agent": "BioETL/1.0 (UnifiedAPIClient)",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        },
        description="Default headers that will be sent with each request.",
    )
    max_url_length: PositiveInt = Field(
        default=2000,
        description="Maximum URL length before falling back to POST with X-HTTP-Method-Override.",
    )


class HTTPConfig(BaseModel):
    """Top-level HTTP configuration section."""

    model_config = ConfigDict(extra="forbid")

    default: HTTPClientConfig = Field(
        ..., description="Baseline client settings applied unless a profile overrides them."
    )
    profiles: MutableMapping[str, HTTPClientConfig] = Field(
        default_factory=dict,
        description="Named profiles that can be referenced by sources.",
    )
