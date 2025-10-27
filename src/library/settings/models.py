"""Application configuration using Pydantic models.

This module provides a typed wrapper around configuration values. Configuration
values are loaded from a YAML file and can be overridden by environment
variables or command line options. The order of precedence is::

    YAML < config.local.yaml < environment variables < CLI overrides

Environment variables follow the ``CHEMBL_DA__SECTION__KEY`` naming pattern
where sections and keys are joined by double underscores. A number of short
aliases are supported; see ``_ALIAS_MAP`` for the full list.
"""

from __future__ import annotations

import random
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator


@dataclass(slots=True)
class ConfigSource:
    """Origin of a configuration value used in :class:`ConfigMetadata`."""

    source: str
    detail: str | None = None


@dataclass(slots=True)
class ConfigMetadata:
    """Metadata describing configuration values and their provenance."""

    snapshot: dict[str, Any]
    sources: dict[tuple[str, ...], ConfigSource]
    cli_paths: dict[str, tuple[str, ...]] = field(default_factory=dict)
    env_warnings: list[str] = field(default_factory=list)

    def get(self, path: str | tuple[str, ...]) -> dict[str, Any]:
        """Return configuration values for the given path."""
        if isinstance(path, str):
            path_tuple = (path,)
        else:
            path_tuple = tuple(path)
        # Type ignore needed because dict.get expects str key, but we're using tuple
        return self.snapshot.get(path_tuple, {})  # type: ignore[arg-type,no-any-return,call-overload]


class ApiCfg(BaseModel):
    """Global API configuration settings."""

    model_config = {"extra": "forbid"}

    base_url: str = Field(default="https://www.ebi.ac.uk/chembl/api/data")
    user_agent: str = Field(default="ChEMBL-Data-Acquisition/1.0")
    timeout_connect: float = Field(default=10.0, ge=0.1, le=300.0)
    timeout_read: float = Field(default=30.0, ge=0.1, le=600.0)
    verify: bool | str = Field(default=True)
    retries: int = Field(default=3, ge=0, le=10)
    backoff_multiplier: float = Field(default=2.0, ge=1.0, le=10.0)
    backoff_jitter_seed: int = Field(default=42, ge=0)  # For determinism

    @field_validator("verify")
    @classmethod
    def validate_verify(cls, v: Any) -> bool | str:
        """Coerce verification config values to ``bool`` or path string."""
        if v is None:
            return True
        if isinstance(v, bool):
            return v
        if isinstance(v, Path):
            return str(v)
        if isinstance(v, str):
            stripped = v.strip()
            if not stripped:
                return True
            lowered = stripped.lower()
            truthy = {"1", "true", "yes", "on"}
            falsy = {"0", "false", "no", "off"}
            if lowered in truthy | falsy:
                return lowered in truthy
            return stripped
        return v

    def build_jitter(self) -> Callable[[float], float]:
        """Return a deterministic jitter function with fixed seed."""
        # Use thread-local random to avoid race conditions
        local_random = threading.local()

        def jitter(delay: float) -> float:
            if not hasattr(local_random, "rng"):
                local_random.rng = random.Random(self.backoff_jitter_seed)  # noqa: S311
            return local_random.rng.uniform(0.5, 1.5) * delay

        return jitter


class RetryCfg(BaseModel):
    """Retry configuration for HTTP requests."""

    model_config = {"extra": "forbid"}

    retries: int = Field(default=3, ge=0, le=10)
    backoff_multiplier: float = Field(default=2.0, ge=1.0, le=10.0)
    backoff_jitter_seed: int = Field(default=42, ge=0)
    max_delay: float = Field(default=60.0, ge=1.0, le=300.0)

    def build_jitter(self) -> Callable[[float], float]:
        """Return a deterministic jitter function with fixed seed."""
        local_random = threading.local()

        def jitter(delay: float) -> float:
            if not hasattr(local_random, "rng"):
                local_random.rng = random.Random(self.backoff_jitter_seed)  # noqa: S311
            return local_random.rng.uniform(0.5, 1.5) * delay

        return jitter


class ChemblCacheCfg(BaseModel):
    """ChEMBL-specific cache configuration."""

    model_config = {"extra": "forbid"}

    cache_ttl: int = Field(default=3600, ge=60, le=86400)  # 1 hour to 1 day
    cache_size: int = Field(default=1000, ge=10, le=10000)


class UniprotMappingCfg(BaseModel):
    """UniProt mapping configuration."""

    model_config = {"extra": "forbid"}

    enabled: bool = Field(default=True)
    batch_size: int = Field(default=25, ge=1, le=100)
    timeout: float = Field(default=30.0, ge=5.0, le=120.0)
    max_retries: int = Field(default=3, ge=0, le=10)


class RateLimitCfg(BaseModel):
    """Rate limiting configuration."""

    model_config = {"extra": "forbid"}

    enabled: bool = Field(default=True)
    global_rps: float = Field(default=10.0, ge=0.1, le=100.0)
    global_burst: int = Field(default=20, ge=1, le=100)
    chembl_rps: float = Field(default=5.0, ge=0.1, le=50.0)
    chembl_burst: int = Field(default=10, ge=1, le=50)


class TargetChemblBatchRetryCfg(BaseModel):
    """Target ChEMBL batch retry configuration."""

    model_config = {"extra": "forbid"}

    chunk_size: int = Field(default=5, ge=1, le=25)
    timeout: float = Field(default=30.0, ge=5.0, le=120.0)
    max_retries: int = Field(default=3, ge=0, le=10)
    enable_split_fallback: bool = Field(default=True)


class Config(BaseModel):
    """Main application configuration."""

    model_config = {"extra": "forbid"}

    api: ApiCfg = Field(default_factory=ApiCfg)
    retry: RetryCfg = Field(default_factory=RetryCfg)
    chembl_cache: ChemblCacheCfg = Field(default_factory=ChemblCacheCfg)
    uniprot_mapping: UniprotMappingCfg = Field(default_factory=UniprotMappingCfg)
    rate_limit: RateLimitCfg = Field(default_factory=RateLimitCfg)
    target_chembl_batch_retry: TargetChemblBatchRetryCfg = Field(default_factory=TargetChemblBatchRetryCfg)

    def get_metadata(self) -> ConfigMetadata:
        """Return metadata about this configuration."""
        return ConfigMetadata(snapshot=self.model_dump(), sources={}, cli_paths={}, env_warnings=[])
