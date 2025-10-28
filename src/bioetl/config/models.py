"""Pydantic models for pipeline configuration."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    headers: Dict[str, str] = Field(default_factory=dict)


class SourceConfig(BaseModel):
    """Data source configuration."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    base_url: str
    batch_size: Optional[int] = Field(default=None, ge=1)
    max_url_length: Optional[int] = Field(default=None, ge=1)
    api_key: Optional[str] = None


class CacheConfig(BaseModel):
    """Cache configuration."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool
    directory: Path
    ttl: int = Field(..., ge=0)
    release_scoped: bool = True


class PathConfig(BaseModel):
    """Path configuration."""

    model_config = ConfigDict(extra="forbid")

    input_root: Path = Field(default=Path("data/input"))
    output_root: Path = Field(default=Path("data/output"))
    cache_root: Optional[Path] = None


class SortConfig(BaseModel):
    """Sorting configuration for determinism."""

    model_config = ConfigDict(extra="forbid")

    by: List[str] = Field(default_factory=list)
    ascending: List[bool] = Field(default_factory=list)


class DeterminismConfig(BaseModel):
    """Determinism configuration."""

    model_config = ConfigDict(extra="forbid")

    sort: SortConfig = Field(default_factory=SortConfig)
    column_order: List[str] = Field(default_factory=list)


class QCConfig(BaseModel):
    """Quality control configuration."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    severity_threshold: str = Field(default="warning")
    thresholds: Dict[str, float] = Field(default_factory=dict)


class PostprocessConfig(BaseModel):
    """Postprocessing configuration."""

    model_config = ConfigDict(extra="allow")

    normalization: Dict[str, Any] = Field(default_factory=dict)
    enrichment: Dict[str, Any] = Field(default_factory=dict)
    deduplication: Dict[str, Any] = Field(default_factory=dict)


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
    http: Dict[str, HttpConfig]
    sources: Dict[str, SourceConfig] = Field(default_factory=dict)
    cache: CacheConfig
    paths: PathConfig
    determinism: DeterminismConfig
    postprocess: PostprocessConfig = Field(default_factory=PostprocessConfig)
    qc: QCConfig = Field(default_factory=QCConfig)
    cli: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("version")
    @classmethod
    def check_version(cls, value: int) -> int:
        """Validate configuration version."""
        if value != 1:
            raise ValueError("Unsupported config version")
        return value

    def model_dump_canonical(self) -> Dict[str, Any]:
        """
        Dump model to dict with canonical serialization for hashing.

        Excludes paths and secrets.
        """
        data = self.model_dump(mode="json", exclude={"paths", "sources"})
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


