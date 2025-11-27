"""Configuration for HTTP clients."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class APIConfig:
    """Configuration for the UnifiedAPIClient."""

    base_url: str
    timeout_sec: float
    max_retries: int
    backoff_factor: float
    max_backoff_sec: float
    rate_limit_calls: int
    rate_limit_period_sec: float
    cache_enabled: bool
    cache_ttl_sec: int
    circuit_breaker_fail_max: int
    circuit_breaker_reset_sec: int
    default_headers: dict[str, str] = field(default_factory=dict)
    user_agent: str = "bioetl-http-client"
