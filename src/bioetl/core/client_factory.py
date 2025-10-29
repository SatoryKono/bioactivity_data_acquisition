"""Factory helpers for constructing HTTP clients from configuration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from bioetl.config.models import (
    CacheConfig,
    CircuitBreakerConfig,
    FallbackOptions,
    HttpConfig,
    PipelineConfig,
    RateLimitConfig,
    RetryConfig,
    TargetSourceConfig,
)
from bioetl.core.api_client import APIConfig

_DEFAULT_RETRY_CONFIG = RetryConfig(
    total=3,
    backoff_multiplier=2.0,
    backoff_max=60.0,
    statuses=[429, 500, 502, 503, 504],
)

_DEFAULT_RATE_LIMIT_CONFIG = RateLimitConfig(max_calls=1, period=1.0)


def ensure_target_source_config(
    source: TargetSourceConfig | Mapping[str, Any] | None,
    *,
    defaults: Mapping[str, Any] | None = None,
) -> TargetSourceConfig:
    """Coerce arbitrary source definitions into a validated ``TargetSourceConfig``."""

    base: dict[str, Any] = dict(defaults or {})

    if isinstance(source, TargetSourceConfig):
        payload = source.model_dump(mode="json")
        base.update(payload)
        return TargetSourceConfig.model_validate(base)

    if isinstance(source, Mapping):
        base.update(source)
        return TargetSourceConfig.model_validate(base)

    return TargetSourceConfig.model_validate(base)


class APIClientFactory:
    """Build :class:`~bioetl.core.api_client.APIConfig` instances for sources."""

    def __init__(
        self,
        *,
        http_profiles: Mapping[str, HttpConfig] | None,
        cache_config: CacheConfig,
        fallback_options: FallbackOptions,
    ) -> None:
        self._http_profiles = dict(http_profiles or {})
        self._cache = cache_config
        self._fallbacks = fallback_options

    @classmethod
    def from_pipeline_config(cls, config: PipelineConfig) -> APIClientFactory:
        """Create a factory bound to the provided pipeline configuration."""

        return cls(
            http_profiles=config.http,
            cache_config=config.cache,
            fallback_options=config.fallbacks,
        )

    def create(self, source_name: str, source_config: TargetSourceConfig) -> APIConfig:
        """Materialize an :class:`APIConfig` for the given source."""

        http_profile = self._resolve_http_profile(source_name, source_config)
        global_http = self._http_profiles.get("global")

        timeout_sec = source_config.timeout_sec
        if timeout_sec is None and http_profile is not None:
            timeout_sec = http_profile.timeout_sec
        if timeout_sec is None and global_http is not None:
            timeout_sec = global_http.timeout_sec
        if timeout_sec is None:
            timeout_sec = 60.0

        connect_timeout = self._resolve_timeout(
            http_profile,
            global_http,
            "connect_timeout_sec",
            timeout_sec,
        )
        read_timeout = self._resolve_timeout(
            http_profile,
            global_http,
            "read_timeout_sec",
            timeout_sec,
        )

        retries = self._resolve_retries(http_profile, global_http)
        rate_limit = self._resolve_rate_limit(source_config, http_profile, global_http)
        rate_limit_jitter = self._resolve_rate_limit_jitter(source_config, http_profile, global_http)

        cache_enabled = (
            source_config.cache_enabled
            if source_config.cache_enabled is not None
            else self._cache.enabled
        )
        cache_ttl = (
            source_config.cache_ttl
            if source_config.cache_ttl is not None
            else self._cache.ttl
        )
        cache_maxsize = (
            source_config.cache_maxsize
            if source_config.cache_maxsize is not None
            else getattr(self._cache, "maxsize", 1024)
        )

        fallback_config = self._fallbacks
        fallback_enabled = fallback_config.enabled
        fallback_strategies = (
            source_config.fallback_strategies
            if source_config.fallback_strategies
            else fallback_config.strategies
        )
        partial_retry_max = (
            source_config.partial_retry_max
            if source_config.partial_retry_max is not None
            else fallback_config.partial_retry_max
        )
        circuit_breaker: CircuitBreakerConfig = (
            source_config.circuit_breaker
            if source_config.circuit_breaker is not None
            else fallback_config.circuit_breaker
        )

        headers: dict[str, str] = {}
        if global_http is not None:
            headers.update(global_http.headers)
        if http_profile is not None:
            headers.update(http_profile.headers)
        headers.update(source_config.headers)

        return APIConfig(
            name=source_name,
            base_url=source_config.base_url,
            headers=headers,
            cache_enabled=cache_enabled,
            cache_ttl=cache_ttl,
            cache_maxsize=cache_maxsize,
            rate_limit_max_calls=rate_limit.max_calls,
            rate_limit_period=rate_limit.period,
            rate_limit_jitter=rate_limit_jitter,
            retry_total=retries.total,
            retry_backoff_factor=retries.backoff_multiplier,
            retry_backoff_max=retries.backoff_max,
            retry_status_codes=[int(code) for code in (retries.statuses or [])],
            partial_retry_max=partial_retry_max,
            timeout_connect=connect_timeout,
            timeout_read=read_timeout,
            cb_failure_threshold=circuit_breaker.failure_threshold,
            cb_timeout=circuit_breaker.timeout_sec,
            fallback_enabled=fallback_enabled,
            fallback_strategies=fallback_strategies,
        )

    def _resolve_http_profile(
        self,
        source_name: str,
        source_config: TargetSourceConfig,
    ) -> HttpConfig | None:
        if source_config.http is not None:
            return source_config.http
        profile_name = source_config.http_profile or source_name
        if profile_name and profile_name in self._http_profiles:
            return self._http_profiles[profile_name]
        return None

    @staticmethod
    def _resolve_timeout(
        http_profile: HttpConfig | None,
        global_http: HttpConfig | None,
        attr: str,
        default: float,
    ) -> float:
        profile_value = getattr(http_profile, attr) if http_profile else None
        if profile_value is not None:
            return float(profile_value)

        global_value = getattr(global_http, attr) if global_http else None
        if global_value is not None:
            return float(global_value)

        return float(default)

    @staticmethod
    def _resolve_retries(
        http_profile: HttpConfig | None,
        global_http: HttpConfig | None,
    ) -> RetryConfig:
        if http_profile is not None:
            return http_profile.retries
        if global_http is not None:
            return global_http.retries
        return _DEFAULT_RETRY_CONFIG

    @staticmethod
    def _resolve_rate_limit(
        source_config: TargetSourceConfig,
        http_profile: HttpConfig | None,
        global_http: HttpConfig | None,
    ) -> RateLimitConfig:
        if source_config.rate_limit is not None:
            return source_config.rate_limit
        if http_profile is not None:
            return http_profile.rate_limit
        if global_http is not None:
            return global_http.rate_limit
        return _DEFAULT_RATE_LIMIT_CONFIG

    @staticmethod
    def _resolve_rate_limit_jitter(
        source_config: TargetSourceConfig,
        http_profile: HttpConfig | None,
        global_http: HttpConfig | None,
    ) -> bool:
        if source_config.http is not None:
            return source_config.http.rate_limit_jitter
        if http_profile is not None:
            return http_profile.rate_limit_jitter
        if global_http is not None:
            return global_http.rate_limit_jitter
        return True
