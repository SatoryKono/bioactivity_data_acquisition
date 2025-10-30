"""Pydantic models for pipeline configuration."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
)

SUPPORTED_FALLBACK_STRATEGIES: tuple[str, ...] = ("cache", "partial_retry")


def _normalise_fallback_strategies(strategies: Iterable[str]) -> list[str]:
    """Validate and de-duplicate fallback strategies while preserving order."""

    normalised: list[str] = []
    for strategy in strategies:
        if strategy not in SUPPORTED_FALLBACK_STRATEGIES:
            supported = ", ".join(SUPPORTED_FALLBACK_STRATEGIES)
            raise ValueError(
                f"Unknown fallback strategy '{strategy}'. Supported strategies: {supported}"
            )
        if strategy not in normalised:
            normalised.append(strategy)
    return normalised


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
    rate_limit_jitter: bool | None = None
    timeout_sec: float | None = Field(default=None, gt=0)
    headers: dict[str, str] = Field(default_factory=dict)
    cache_enabled: bool | None = None
    cache_ttl: int | None = Field(default=None, ge=0)
    cache_maxsize: int | None = Field(default=None, ge=1)
    stage: str = Field(default="primary")
    partial_retry_max: int | None = Field(default=None, ge=0)
    fallback_strategies: list[str] = Field(default_factory=list)
    circuit_breaker: CircuitBreakerConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def extract_rate_limit_jitter(cls, data: Any) -> Any:
        """Lift ``http.rate_limit_jitter`` into a top-level field."""

        if not isinstance(data, dict):
            return data

        http_block = data.get("http")
        if isinstance(http_block, dict) and "rate_limit_jitter" in http_block:
            payload = dict(data)
            http_payload = dict(http_block)
            payload["rate_limit_jitter"] = http_payload.pop("rate_limit_jitter")

            if http_payload:
                payload["http"] = http_payload
            else:
                payload.pop("http", None)

            return payload

        return data

    @field_validator("api_key", mode="before")
    @classmethod
    def resolve_api_key(cls, value: str | None) -> str | None:
        """Resolve environment variable references for API keys."""

        if value is None:
            return value

        if isinstance(value, str):
            candidate = value.strip()
            if candidate.startswith("env:"):
                reference = candidate.split(":", 1)[1]
                env_value = cls._resolve_api_key_reference(reference)
                if env_value is None:
                    raise ValueError(
                        f"Environment variable '{reference.split(':', 1)[0]}' referenced for api_key is not set"
                    )
                return env_value

            if candidate.startswith("${") and candidate.endswith("}"):
                reference = candidate[2:-1]
                env_value = cls._resolve_api_key_reference(reference)
                if env_value is None:
                    raise ValueError(
                        f"Environment variable '{reference.split(':', 1)[0]}' referenced for api_key is not set"
                    )
                return env_value

        return value

    @staticmethod
    def _resolve_api_key_reference(reference: str) -> str | None:
        """Resolve ``ENV`` style references allowing optional defaults."""

        parts = reference.split(":", 1)
        env_name = parts[0].strip()
        if not env_name:
            raise ValueError("Environment reference missing variable name for api_key")

        default_value: str | None = None
        if len(parts) == 2:
            default_value = parts[1]

        env_value = os.getenv(env_name)
        if env_value is not None:
            return env_value

        return default_value

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
                    if env_value is None:
                        raise ValueError(
                            f"Environment variable '{env_name}' required for header '{key}' is not set"
                        )
                    resolved[key] = env_value
                    continue

                if candidate.startswith("${") and candidate.endswith("}"):
                    env_name = candidate[2:-1]
                    env_value = os.getenv(env_name)
                    if env_value is None:
                        raise ValueError(
                            f"Environment variable '{env_name}' required for header '{key}' is not set"
                        )
                    resolved[key] = env_value
                    continue

                resolved[key] = header_value
            else:
                resolved[key] = header_value

        return resolved

    @field_validator("fallback_strategies")
    @classmethod
    def validate_fallback_strategies(cls, value: list[str]) -> list[str]:
        """Ensure source-level fallback strategies are supported."""

        return _normalise_fallback_strategies(value)


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
    hash_policy_version: str = Field(
        default="1.0.0",
        description="Version identifier for the canonical hashing policy",
    )


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


class MaterializationStageTemplate(BaseModel):
    """Template describing a materialization stage."""

    model_config = ConfigDict(extra="forbid")

    format: str = Field(default="parquet")
    filename: str | None = None
    description: str | None = None


class MaterializationStage(BaseModel):
    """Concrete materialization stage settings."""

    model_config = ConfigDict(extra="forbid")

    format: str | None = None
    path: Path | str | None = None
    filename: str | None = None
    description: str | None = None

    @field_validator("path", mode="before")
    @classmethod
    def coerce_path(cls, value: Path | str | None) -> Path | str | None:
        """Allow plain strings to be used as direct stage paths."""

        if value is None:
            return None
        if isinstance(value, Path):
            return value
        if isinstance(value, str):
            return value
        raise TypeError("Stage path must be a string or Path")

    @model_validator(mode="before")
    @classmethod
    def coerce_from_scalar(cls, value: Any) -> Any:
        """Permit shorthand strings when defining stage paths."""

        if isinstance(value, (str, Path)):
            return {"path": value}
        return value

    def with_defaults(self, template: MaterializationStageTemplate | None) -> MaterializationStage:
        """Return a copy of the stage, enriched with template defaults."""

        if template is None:
            return MaterializationStage(**self.model_dump())
        stage_data = {
            key: value for key, value in self.model_dump().items() if value is not None
        }
        template_data = template.model_dump()
        merged = {**template_data, **stage_data}
        return MaterializationStage(**merged)


class MaterializationDataset(BaseModel):
    """Collection of stage settings for a named dataset."""

    model_config = ConfigDict(extra="allow")

    root: Path | str | None = None
    bronze: MaterializationStage | None = None
    silver: MaterializationStage | None = None
    gold: MaterializationStage | None = None
    qc_report: MaterializationStage | None = None
    staging: MaterializationStage | None = None
    checkpoints: dict[str, Path | str] = Field(default_factory=dict)

    @field_validator("root", mode="before")
    @classmethod
    def coerce_root(cls, value: Path | str | None) -> Path | str | None:
        """Normalise dataset root representation."""

        if value is None or isinstance(value, (Path, str)):
            return value
        raise TypeError("Dataset root must be a string or Path")


class MaterializationDefaults(BaseModel):
    """Default values applied to materialization datasets."""

    model_config = ConfigDict(extra="allow")

    dataset: str | None = None
    root: Path | str | None = None


class MaterializationPaths(BaseModel):
    """Paths used for materialization stages."""

    model_config = ConfigDict(extra="allow")

    bronze: MaterializationStage | None = None
    silver: MaterializationStage | None = None
    gold: MaterializationStage | None = None
    qc_report: MaterializationStage | None = None
    staging: MaterializationStage | None = None
    checkpoints: dict[str, Path | str] = Field(default_factory=dict)
    stages: dict[str, MaterializationStageTemplate] = Field(default_factory=dict)
    datasets: dict[str, MaterializationDataset] = Field(default_factory=dict)
    defaults: MaterializationDefaults = Field(default_factory=MaterializationDefaults)

    @field_validator("bronze", "silver", "gold", "qc_report", "staging", mode="before")
    @classmethod
    def coerce_stage(cls, value: Any) -> Any:
        """Allow shorthand definitions for top-level stages."""

        if isinstance(value, (str, Path)):
            return {"path": value}
        return value

    @field_validator("checkpoints", mode="before")
    @classmethod
    def coerce_checkpoints(cls, value: Any) -> dict[str, Path | str]:
        """Ensure checkpoints resolve to simple path mappings."""

        if value is None:
            return {}
        if not isinstance(value, dict):
            raise TypeError("checkpoints must be a mapping of names to paths")
        return value

    def _format_with_context(
        self,
        value: str,
        dataset: str | None,
        pipeline: PipelineMetadata | None,
    ) -> str:
        """Substitute well-known placeholders within strings."""

        context: dict[str, str] = {}
        if dataset:
            context["dataset"] = dataset
        if pipeline is not None:
            context.setdefault("entity", pipeline.entity)
            context.setdefault("pipeline", pipeline.name)

        try:
            return value.format(**context)
        except KeyError:
            return value

    def _resolve_dataset_key(
        self, dataset: str | None, pipeline: PipelineMetadata | None
    ) -> str | None:
        """Determine the dataset identifier when omitted."""

        if dataset:
            return dataset
        default_dataset = self.defaults.dataset
        if isinstance(default_dataset, str):
            return self._format_with_context(default_dataset, dataset=None, pipeline=pipeline)
        if pipeline is not None:
            return pipeline.entity
        return None

    def _resolve_dataset_root(
        self,
        dataset: str | None,
        paths: PathConfig | None,
        pipeline: PipelineMetadata | None,
    ) -> Path | None:
        """Compute the on-disk directory for a dataset."""

        base_root = Path(paths.output_root) if paths is not None else None
        dataset_key = self._resolve_dataset_key(dataset, pipeline)
        dataset_config = self.datasets.get(dataset_key) if dataset_key else None

        root_value: Path | str | None = None
        if dataset_config and dataset_config.root is not None:
            root_value = dataset_config.root
        elif self.defaults.root is not None:
            root_value = self.defaults.root

        if root_value is None:
            return base_root

        root_str = str(root_value)
        formatted = self._format_with_context(root_str, dataset_key, pipeline)
        resolved_root = Path(formatted)

        if resolved_root.is_absolute():
            return resolved_root
        if base_root is not None:
            return base_root / resolved_root
        return resolved_root

    def _resolve_value_path(
        self,
        value: Path | str,
        dataset: str | None,
        pipeline: PipelineMetadata | None,
        paths: PathConfig | None,
    ) -> Path:
        """Normalise potentially relative path specifications."""

        value_str = str(value)
        formatted = self._format_with_context(value_str, dataset, pipeline)
        candidate = Path(formatted)
        if candidate.is_absolute():
            return candidate
        root = self._resolve_dataset_root(dataset, paths, pipeline)
        if root is not None:
            return root / candidate
        if paths is not None:
            return Path(paths.output_root) / candidate
        return candidate

    def get_stage(
        self,
        stage: str,
        dataset: str | None = None,
        *,
        pipeline: PipelineMetadata | None = None,
    ) -> MaterializationStage | None:
        """Retrieve a materialization stage configuration."""

        dataset_key = self._resolve_dataset_key(dataset, pipeline)
        stage_template = self.stages.get(stage)

        if dataset_key and dataset_key in self.datasets:
            dataset_cfg = self.datasets[dataset_key]
            stage_cfg = getattr(dataset_cfg, stage, None)
            if stage_cfg is not None:
                return stage_cfg.with_defaults(stage_template)

        stage_cfg = getattr(self, stage, None)
        if stage_cfg is not None:
            return stage_cfg.with_defaults(stage_template)

        if stage_template is not None:
            return MaterializationStage(**stage_template.model_dump())
        return None

    def resolve_stage_path(
        self,
        stage: str,
        dataset: str | None = None,
        *,
        pipeline: PipelineMetadata | None = None,
        paths: PathConfig | None = None,
    ) -> Path | None:
        """Resolve a fully-qualified path for a materialization stage."""

        dataset_key = self._resolve_dataset_key(dataset, pipeline)
        stage_cfg = self.get_stage(stage, dataset_key, pipeline=pipeline)
        if stage_cfg is None:
            return None

        if stage_cfg.path is not None:
            return self._resolve_value_path(stage_cfg.path, dataset_key, pipeline, paths)

        if stage_cfg.filename is None:
            return None

        filename = self._format_with_context(stage_cfg.filename, dataset_key, pipeline)
        root = self._resolve_dataset_root(dataset_key, paths, pipeline)

        if root is None:
            return Path(filename)
        return root / filename

    def resolve_checkpoint_path(
        self,
        name: str,
        dataset: str | None = None,
        *,
        pipeline: PipelineMetadata | None = None,
        paths: PathConfig | None = None,
    ) -> Path | None:
        """Resolve a checkpoint path, preferring dataset-level overrides."""

        dataset_key = self._resolve_dataset_key(dataset, pipeline)
        if dataset_key and dataset_key in self.datasets:
            dataset_cfg = self.datasets[dataset_key]
            if name in dataset_cfg.checkpoints:
                return self._resolve_value_path(
                    dataset_cfg.checkpoints[name], dataset_key, pipeline, paths
                )

        if name in self.checkpoints:
            return self._resolve_value_path(
                self.checkpoints[name], dataset_key, pipeline, paths
            )
        return None


class FallbackOptions(BaseModel):
    """Fallback behaviour toggles for HTTP sources."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    prefer_cache: bool = True
    allow_partial_materialization: bool = False
    use_materialized_outputs: bool = True
    strategies: list[str] = Field(
        default_factory=lambda: list(SUPPORTED_FALLBACK_STRATEGIES)
    )
    partial_retry_max: int = Field(default=3, ge=0)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)

    @field_validator("strategies")
    @classmethod
    def validate_strategies(cls, value: list[str]) -> list[str]:
        """Ensure global fallback strategies are supported."""

        return _normalise_fallback_strategies(value)


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

    _source_path: Path | None = PrivateAttr(default=None)

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

    def resolve_materialization_path(
        self, stage: str, dataset: str | None = None
    ) -> Path | None:
        """Resolve the absolute path for a materialization stage."""

        return self.materialization.resolve_stage_path(
            stage, dataset, pipeline=self.pipeline, paths=self.paths
        )

    def resolve_checkpoint_path(
        self, name: str, dataset: str | None = None
    ) -> Path | None:
        """Resolve the absolute path for a materialization checkpoint."""

        return self.materialization.resolve_checkpoint_path(
            name, dataset, pipeline=self.pipeline, paths=self.paths
        )

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

    @property
    def source_path(self) -> Path | None:
        """Return the resolved path of the originating YAML configuration."""

        return self._source_path

    def attach_source_path(self, path: Path | str | None) -> None:
        """Persist the originating YAML configuration path for metadata exports."""

        resolved: Path | None = None
        if path is not None:
            try:
                resolved = Path(path).resolve()
            except (TypeError, OSError):  # pragma: no cover - defensive guard
                resolved = None
        object.__setattr__(self, "_source_path", resolved)

    def relative_source_path(self, base: Path | None = None) -> Path | None:
        """Return the configuration path relative to ``base`` (defaults to CWD)."""

        if self._source_path is None:
            return None

        base_path = base or Path.cwd()
        try:
            return self._source_path.relative_to(base_path)
        except ValueError:
            try:
                return Path(os.path.relpath(self._source_path, base_path))
            except OSError:
                return self._source_path
