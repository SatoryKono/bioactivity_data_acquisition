"""Pydantic models for pipeline configuration."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class RetryConfig(BaseModel):
    """Retry policy configuration."""

    model_config = ConfigDict(extra="forbid")

    total: int = Field(..., ge=0)
    backoff_multiplier: float = Field(..., gt=0)
    backoff_max: float = Field(..., gt=0)
    statuses: list[int] = Field(
        default_factory=list,
        description="HTTP status codes that should trigger a retry before giving up",
    )


class RateLimitConfig(BaseModel):
    """Rate limiting configuration."""

    model_config = ConfigDict(extra="forbid")

    max_calls: int = Field(..., ge=1)
    period: float = Field(..., gt=0)


class HttpConfig(BaseModel):
    """HTTP configuration."""

    model_config = ConfigDict(extra="forbid")

    timeout_sec: float = Field(..., gt=0)
    connect_timeout_sec: float | None = Field(default=None, gt=0)
    read_timeout_sec: float | None = Field(default=None, gt=0)
    retries: RetryConfig
    rate_limit: RateLimitConfig
    rate_limit_jitter: bool = True
    headers: dict[str, str] = Field(default_factory=dict)


class SourceConfig(BaseModel):
    """Data source configuration."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    base_url: str
    batch_size: int | None = Field(default=None, ge=1)
    max_url_length: int | None = Field(default=None, ge=1)
    api_key: str | None = None


class CircuitBreakerConfig(BaseModel):
    """Circuit breaker tuning options."""

    model_config = ConfigDict(extra="forbid")

    failure_threshold: int = Field(default=5, ge=1)
    timeout_sec: float = Field(default=60.0, gt=0)


class TargetSourceConfig(SourceConfig):
    """Extended configuration for target pipeline sources."""

    model_config = ConfigDict(extra="allow")

    http_profile: str | None = None
    http: HttpConfig | None = None
    rate_limit: RateLimitConfig | None = None
    timeout_sec: float | None = Field(default=None, gt=0)
    headers: dict[str, str] = Field(default_factory=dict)
    cache_enabled: bool | None = None
    cache_ttl: int | None = Field(default=None, ge=0)
    cache_maxsize: int | None = Field(default=None, ge=1)
    stage: str = Field(default="primary")
    partial_retry_max: int | None = Field(default=None, ge=0)
    fallback_strategies: list[str] = Field(default_factory=list)
    circuit_breaker: CircuitBreakerConfig | None = None

    @field_validator("api_key", mode="before")
    @classmethod
    def resolve_api_key(cls, value: str | None) -> str | None:
        """Resolve environment variable references for API keys."""

        if value is None:
            return value

        if isinstance(value, str):
            candidate = value.strip()
            if candidate.startswith("env:"):
                env_name = candidate.split(":", 1)[1]
                env_value = os.getenv(env_name)
                return env_value if env_value is not None else value

            if candidate.startswith("${") and candidate.endswith("}"):
                env_name = candidate[2:-1]
                env_value = os.getenv(env_name)
                return env_value if env_value is not None else value

        return value

    @field_validator("headers", mode="after")
    @classmethod
    def resolve_header_secrets(cls, value: dict[str, str]) -> dict[str, str]:
        """Resolve environment references in header values."""

        resolved: dict[str, str] = {}
        for key, header_value in value.items():
            if isinstance(header_value, str):
                candidate = header_value.strip()
                if candidate.startswith("env:"):
                    env_name = candidate.split(":", 1)[1]
                    env_value = os.getenv(env_name)
                    if env_value is not None:
                        resolved[key] = env_value
                        continue

                if candidate.startswith("${") and candidate.endswith("}"):
                    env_name = candidate[2:-1]
                    env_value = os.getenv(env_name)
                    if env_value is not None:
                        resolved[key] = env_value
                        continue

                resolved[key] = header_value
            else:
                resolved[key] = header_value

        return resolved


class CacheConfig(BaseModel):
    """Cache configuration."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool
    directory: Path | str
    ttl: int = Field(..., ge=0)
    release_scoped: bool = True
    maxsize: int = Field(default=1024, ge=1)


class PathConfig(BaseModel):
    """Path configuration."""

    model_config = ConfigDict(extra="forbid")

    input_root: Path = Field(default=Path("data/input"))
    output_root: Path = Field(default=Path("data/output"))
    cache_root: Path | None = None


class SortConfig(BaseModel):
    """Sorting configuration for determinism."""

    model_config = ConfigDict(extra="forbid")

    by: list[str] = Field(default_factory=list)
    ascending: list[bool] = Field(default_factory=list)


class DeterminismConfig(BaseModel):
    """Determinism configuration."""

    model_config = ConfigDict(extra="forbid")

    hash_algorithm: str = Field(default="sha256", description="Hash algorithm for row integrity")
    float_precision: int = Field(default=6, ge=0, le=15, description="Float precision for canonical serialization")
    datetime_format: str = Field(default="iso8601", description="DateTime format for canonical serialization")
    sort: SortConfig = Field(default_factory=SortConfig)
    column_order: list[str] = Field(default_factory=list)


class QCConfig(BaseModel):
    """Quality control configuration."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    severity_threshold: str = Field(default="warning")
    thresholds: dict[str, Any] = Field(default_factory=dict)
    enrichments: dict[str, Any] = Field(default_factory=dict)


class PostprocessConfig(BaseModel):
    """Postprocessing configuration."""

    model_config = ConfigDict(extra="allow")

    normalization: dict[str, Any] = Field(default_factory=dict)
    enrichment: dict[str, Any] = Field(default_factory=dict)
    deduplication: dict[str, Any] = Field(default_factory=dict)


class PipelineMetadata(BaseModel):
    """Pipeline metadata."""

    model_config = ConfigDict(extra="forbid")

    name: str
    entity: str
    release_scope: bool = True


class MaterializationPaths(BaseModel):
    """Paths used for materialization stages."""

    model_config = ConfigDict(extra="allow")

    bronze: Path | str | None = None
    silver: Path | str | None = None
    gold: Path | str | None = None
    qc_report: Path | str | None = None
    staging: Path | str | None = None
    checkpoints: dict[str, Path | str] = Field(default_factory=dict)


class FallbackOptions(BaseModel):
    """Fallback behaviour toggles for HTTP sources."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    prefer_cache: bool = True
    allow_partial_materialization: bool = False
    use_materialized_outputs: bool = True
    strategies: list[str] = Field(default_factory=lambda: ["cache", "network"])
    partial_retry_max: int = Field(default=3, ge=0)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)


class PipelineConfig(BaseModel):
    """Root pipeline configuration model."""

    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    version: int
    pipeline: PipelineMetadata
    http: dict[str, HttpConfig]
    sources: dict[str, TargetSourceConfig] = Field(default_factory=dict)
    cache: CacheConfig
    paths: PathConfig
    determinism: DeterminismConfig
    postprocess: PostprocessConfig = Field(default_factory=PostprocessConfig)
    qc: QCConfig = Field(default_factory=QCConfig)
    materialization: MaterializationPaths = Field(default_factory=MaterializationPaths)
    fallbacks: FallbackOptions = Field(default_factory=FallbackOptions)
    cli: dict[str, Any] = Field(default_factory=dict)

    @field_validator("version")
    @classmethod
    def check_version(cls, value: int) -> int:
        """Validate configuration version."""
        if value != 1:
            raise ValueError("Unsupported config version")
        return value

    @model_validator(mode="after")
    def validate_chembl_batch_size(self) -> PipelineConfig:
        """Ensure ChEMBL batch size has a sensible default and stays within limits."""

        chembl = self.sources.get("chembl") if self.sources else None
        if chembl is None:
            return self

        batch_size = getattr(chembl, "batch_size", None)
        if batch_size is None:
            chembl.batch_size = 25
            return self

        if batch_size > 25:
            raise ValueError(
                "sources.chembl.batch_size must be <= 25 due to ChEMBL API constraints",
            )

        return self

    def model_dump_canonical(self) -> dict[str, Any]:
        """
        Dump model to dict with canonical serialization for hashing.

        Excludes paths and secrets.
        """
        data: dict[str, Any] = self.model_dump(mode="json", exclude={"paths", "sources"})
        # Add back sources without api_key
        if self.sources:
            data["sources"] = {}
            for key, source in self.sources.items():
                source_data = source.model_dump(mode="json", exclude={"api_key"})
                data["sources"][key] = source_data
        return data

    @property
    def config_hash(self) -> str:
        """Compute SHA256 hash of canonical configuration."""
        canonical = self.model_dump_canonical()
        json_str = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(json_str.encode()).hexdigest()

