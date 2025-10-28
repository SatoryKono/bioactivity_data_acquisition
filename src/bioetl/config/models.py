"""Pydantic models for pipeline configuration."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class RetryConfig(BaseModel):
    """Retry policy configuration."""

    model_config = ConfigDict(extra="forbid")

    total: int = Field(..., ge=0)
    backoff_multiplier: float = Field(..., gt=0)
    backoff_max: float = Field(..., gt=0)


class RateLimitConfig(BaseModel):
    """Rate limiting configuration."""

    model_config = ConfigDict(extra="forbid")

    max_calls: int = Field(..., ge=1)
    period: float = Field(..., gt=0)


class HttpConfig(BaseModel):
    """HTTP configuration."""

    model_config = ConfigDict(extra="forbid")

    timeout_sec: float = Field(..., gt=0)
    retries: RetryConfig
    rate_limit: RateLimitConfig
    headers: dict[str, str] = Field(default_factory=dict)


class SourceConfig(BaseModel):
    """Data source configuration."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    base_url: str
    batch_size: int | None = Field(default=None, ge=1)
    max_url_length: int | None = Field(default=None, ge=1)
    api_key: str | None = None


class CacheConfig(BaseModel):
    """Cache configuration."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool
    directory: Path | str
    ttl: int = Field(..., ge=0)
    release_scoped: bool = True


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
    thresholds: dict[str, float] = Field(default_factory=dict)


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


class PipelineConfig(BaseModel):
    """Root pipeline configuration model."""

    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    version: int
    pipeline: PipelineMetadata
    http: dict[str, HttpConfig]
    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    cache: CacheConfig
    paths: PathConfig
    determinism: DeterminismConfig
    postprocess: PostprocessConfig = Field(default_factory=PostprocessConfig)
    qc: QCConfig = Field(default_factory=QCConfig)
    cli: dict[str, Any] = Field(default_factory=dict)

    @field_validator("version")
    @classmethod
    def check_version(cls, value: int) -> int:
        """Validate configuration version."""
        if value != 1:
            raise ValueError("Unsupported config version")
        return value

    @model_validator(mode="after")
    def _validate_chembl_batch_size(self) -> "PipelineConfig":
        """Ensure ChEMBL batch size does not exceed API limits."""

        chembl = self.sources.get("chembl") if self.sources else None
        if chembl is not None:
            batch_size = getattr(chembl, "batch_size", None)
            if batch_size is None:
                chembl.batch_size = 25
            elif batch_size > 25:
                raise ValueError(
                    "sources.chembl.batch_size must be <= 25 due to ChEMBL API URL length limit",
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

    @model_validator(mode="after")
    def validate_chembl_batch_size(self) -> "PipelineConfig":
        """Enforce ChEMBL batch size limits at configuration load time."""
        chembl_source = self.sources.get("chembl")
        if chembl_source and chembl_source.batch_size and chembl_source.batch_size > 25:
            raise ValueError(
                "sources.chembl.batch_size must be <= 25 due to ChEMBL API constraints",
            )
        return self


