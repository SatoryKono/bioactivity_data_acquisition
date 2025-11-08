"""Policies governing HTTP access, determinism and fallback behavior."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, PositiveFloat, PositiveInt, model_validator

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
    allowed_methods: tuple[str, ...] = Field(
        default=("GET", "HEAD", "OPTIONS"),
        description="HTTP methods that are considered safe for retries.",
    )
    respect_retry_after: bool = Field(
        default=True,
        description="Whether to honour the Retry-After header when present.",
    )
    retry_on_connection_errors: bool = Field(
        default=True,
        description="Retry when a connection-related error is raised by the transport.",
    )
    retry_on_read_errors: bool = Field(
        default=True,
        description="Retry when a read timeout or similar socket error occurs.",
    )
    retry_on_other_errors: bool = Field(
        default=False,
        description="Retry on any other error raised by the transport stack.",
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
    write_timeout_sec: PositiveFloat = Field(
        default=60.0,
        description="Socket write timeout in seconds.",
    )
    pool_timeout_sec: PositiveFloat = Field(
        default=5.0,
        description="Maximum time to wait for a connection from the pool.",
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
    proxies: Mapping[str, str] | None = Field(
        default=None,
        description="Optional proxy configuration mapping scheme to proxy URL.",
    )
    verify: bool | str = Field(
        default=True,
        description="SSL verification flag or path to CA bundle.",
    )
    cert: str | tuple[str, str] | None = Field(
        default=None,
        description="Client certificate to present for TLS connections.",
    )
    trust_env: bool = Field(
        default=False,
        description="Respect environment variables such as HTTP(S)_PROXY when True.",
    )
    pool_connections: PositiveInt = Field(
        default=10,
        description="Number of connections to cache per host.",
    )
    pool_maxsize: PositiveInt = Field(
        default=10,
        description="Maximum number of connections to save in the pool.",
    )
    pool_block: bool = Field(
        default=True,
        description="Block when the connection pool is exhausted instead of failing fast.",
    )
    max_redirects: PositiveInt = Field(
        default=30,
        description="Maximum number of redirects to follow automatically.",
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


class FallbacksConfig(BaseModel):
    """Global behavior toggles for fallback mechanisms."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=True,
        description="Whether the pipeline should attempt configured fallback strategies.",
    )
    max_depth: PositiveInt | None = Field(
        default=None,
        description="Maximum fallback depth allowed before failing fast.",
    )


class DeterminismSerializationCSVConfig(BaseModel):
    """CSV serialization preferences."""

    model_config = ConfigDict(extra="forbid")

    separator: str = Field(default=",", description="Column separator used when writing CSV files.")
    quoting: str = Field(
        default="ALL", description="Quoting strategy compatible with pandas CSV writer."
    )
    na_rep: str = Field(
        default="", description="String representation for missing values in CSV output."
    )


class DeterminismSerializationConfig(BaseModel):
    """Normalization of serialized outputs."""

    model_config = ConfigDict(extra="forbid")

    csv: DeterminismSerializationCSVConfig = Field(
        default_factory=DeterminismSerializationCSVConfig
    )
    booleans: tuple[str, str] = Field(
        default=("True", "False"),
        description="Canonical string representations for boolean values.",
    )
    nan_rep: str = Field(default="NaN", description="String representation for NaN values.")


class DeterminismSortingConfig(BaseModel):
    """Stable sorting configuration."""

    model_config = ConfigDict(extra="forbid")

    by: list[str] = Field(
        default_factory=list, description="Columns defining the deterministic sort order."
    )
    ascending: list[bool] = Field(
        default_factory=list,
        description="Sort direction per column; defaults to ascending when empty.",
    )
    na_position: str = Field(
        default="last", description="Where to place null values during sorting."
    )


class DeterminismHashColumnSchema(BaseModel):
    """Schema definition for hash columns emitted by the pipeline."""

    model_config = ConfigDict(extra="forbid")

    dtype: str = Field(default="string", description="Тип данных колонки (pandas dtype).")
    length: PositiveInt = Field(default=64, description="Фиксированная длина хэша в символах.")
    nullable: bool = Field(default=False, description="Допускается ли NULL/NA значение.")


class DeterminismHashingConfig(BaseModel):
    """Hashing policy for determinism checks."""

    model_config = ConfigDict(extra="forbid")

    algorithm: str = Field(
        default="sha256", description="Hash algorithm used for row/business key hashes."
    )
    row_fields: Sequence[str] = Field(
        default_factory=tuple,
        description="Columns included in the per-row hash calculation.",
    )
    business_key_fields: Sequence[str] = Field(
        default_factory=tuple,
        description="Columns used to compute the business key hash.",
    )
    exclude_fields: Sequence[str] = Field(
        default_factory=lambda: ("generated_at", "run_id"),
        description="Fields excluded from deterministic hashing.",
    )
    business_key_column: str = Field(
        default="hash_business_key",
        description="Имя колонки с хэшом бизнес-ключа в итоговом датасете.",
    )
    row_hash_column: str = Field(
        default="hash_row",
        description="Имя колонки с хэшом строки в итоговом датасете.",
    )
    business_key_schema: DeterminismHashColumnSchema = Field(
        default_factory=DeterminismHashColumnSchema,
        description="Схема Pandera/проектного контракта для hash_business_key.",
    )
    row_hash_schema: DeterminismHashColumnSchema = Field(
        default_factory=DeterminismHashColumnSchema,
        description="Схема Pandera/проектного контракта для hash_row.",
    )


class DeterminismEnvironmentConfig(BaseModel):
    """Locale and timezone controls for deterministic output."""

    model_config = ConfigDict(extra="forbid")

    timezone: str = Field(default="UTC", description="Timezone enforced during pipeline execution.")
    locale: str = Field(default="C", description="Locale to apply when formatting values.")


class DeterminismWriteConfig(BaseModel):
    """Atomic write strategy."""

    model_config = ConfigDict(extra="forbid")

    strategy: str = Field(default="atomic", description="Write strategy (atomic or direct).")


class DeterminismMetaConfig(BaseModel):
    """Metadata emission controls."""

    model_config = ConfigDict(extra="forbid")

    location: str = Field(
        default="sibling",
        description="Where to store the generated meta.yaml relative to the dataset.",
    )
    include_fields: Sequence[str] = Field(
        default_factory=tuple,
        description="Metadata keys that must always be present.",
    )
    exclude_fields: Sequence[str] = Field(
        default_factory=tuple,
        description="Metadata keys that should be stripped before hashing.",
    )


class DeterminismConfig(BaseModel):
    """Deterministic output guarantees."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="Toggle determinism enforcement.")
    hash_policy_version: str = Field(
        default="1.0.0",
        description="Version of the hashing policy for compatibility tracking.",
    )
    float_precision: PositiveInt = Field(
        default=6,
        description="Number of decimal places to preserve when normalizing floats.",
    )
    datetime_format: str = Field(
        default="iso8601",
        description="Datetime serialization format used throughout the pipeline.",
    )
    column_validation_ignore_suffixes: Sequence[str] = Field(
        default_factory=lambda: ("_scd", "_temp", "_meta", "_tmp"),
        description="Column suffixes ignored during strict schema validation.",
    )
    sort: DeterminismSortingConfig = Field(default_factory=DeterminismSortingConfig)
    column_order: Sequence[str] = Field(
        default_factory=tuple,
        description="Expected column order for the final dataset.",
    )
    serialization: DeterminismSerializationConfig = Field(
        default_factory=DeterminismSerializationConfig
    )
    hashing: DeterminismHashingConfig = Field(default_factory=DeterminismHashingConfig)
    environment: DeterminismEnvironmentConfig = Field(default_factory=DeterminismEnvironmentConfig)
    write: DeterminismWriteConfig = Field(default_factory=DeterminismWriteConfig)
    meta: DeterminismMetaConfig = Field(default_factory=DeterminismMetaConfig)

    @model_validator(mode="after")
    def validate_sorting(self) -> DeterminismConfig:
        if self.sort.ascending and len(self.sort.ascending) != len(self.sort.by):
            msg = "determinism.sort.ascending must be empty or match determinism.sort.by length"
            raise ValueError(msg)
        if len(self.sort.by) != len(set(self.sort.by)):
            msg = "determinism.sort.by must not contain duplicate columns"
            raise ValueError(msg)
        if len(self.column_order) != len(set(self.column_order)):
            msg = "determinism.column_order must not contain duplicate columns"
            raise ValueError(msg)
        return self


__all__ = [
    "StatusCode",
    "RetryConfig",
    "RateLimitConfig",
    "CircuitBreakerConfig",
    "HTTPClientConfig",
    "HTTPConfig",
    "FallbacksConfig",
    "DeterminismSerializationCSVConfig",
    "DeterminismSerializationConfig",
    "DeterminismSortingConfig",
    "DeterminismHashColumnSchema",
    "DeterminismHashingConfig",
    "DeterminismEnvironmentConfig",
    "DeterminismWriteConfig",
    "DeterminismMetaConfig",
    "DeterminismConfig",
]
