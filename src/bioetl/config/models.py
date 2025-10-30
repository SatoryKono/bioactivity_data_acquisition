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

from bioetl.utils.column_validator import (
    DEFAULT_COLUMN_VALIDATION_IGNORE_SUFFIXES,
    normalise_ignore_suffixes,
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
    column_validation_ignore_suffixes: list[str] = Field(
        default_factory=lambda: list(DEFAULT_COLUMN_VALIDATION_IGNORE_SUFFIXES)
    )

    @field_validator("column_validation_ignore_suffixes")
    @classmethod
    def normalise_validation_suffixes(cls, values: list[str]) -> list[str]:
        """Normalise configured suffixes for column validation filters."""

        return list(normalise_ignore_suffixes(values))


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
    version: str
    release_scope: bool = True


class MaterializationFormatConfig(BaseModel):
    """Declarative description of a supported materialization format."""

    model_config = ConfigDict(extra="forbid")

    extension: str = Field(default=".parquet", description="File extension for the format")

    @field_validator("extension")
    @classmethod
    def validate_extension(cls, value: str) -> str:
        """Ensure extensions include a leading dot for compatibility with Path.suffix."""

        value = value.strip()
        if not value:
            raise ValueError("Materialization format extension cannot be empty")
        if not value.startswith("."):
            raise ValueError("Materialization format extension must start with '.'")
        return value


class MaterializationDatasetPaths(BaseModel):
    """Per-dataset materialization configuration."""

    model_config = ConfigDict(extra="forbid")

    path: Path | str | None = None
    filename: str | None = None
    directory: str | None = None
    formats: dict[str, Path | str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalise_formats(self) -> "MaterializationDatasetPaths":
        """Normalise format keys to lower-case for consistent lookups."""

        if self.formats:
            normalised = {str(key).strip().lower(): value for key, value in self.formats.items()}
            object.__setattr__(self, "formats", normalised)
        return self

    def resolve(
        self,
        *,
        stage: "MaterializationStagePaths",
        registry: "MaterializationPaths",
        dataset: str,
        format: str,
    ) -> Path | None:
        """Resolve the on-disk path for the dataset in the requested format."""

        format_key = format.strip().lower()

        candidate: Path | str | None = self.formats.get(format_key)
        if candidate is not None:
            return registry._normalise_path(candidate)

        if self.path is not None:
            return registry._normalise_path(self.path)

        directory = self.directory or stage.directory
        filename = self.filename or dataset

        tentative = Path(directory) / filename if directory else Path(filename)

        if not tentative.suffix:
            tentative = tentative.with_suffix(registry.extension_for(format))

        return registry._normalise_path(tentative)

    def infer_default_format(
        self,
        *,
        stage: "MaterializationStagePaths",
        registry: "MaterializationPaths",
    ) -> str | None:
        """Infer the preferred format for the dataset from config state."""

        if self.formats:
            if stage.format and stage.format in self.formats:
                return stage.format
            if registry.default_format in self.formats:
                return registry.default_format
            return next(iter(self.formats.keys()))

        for candidate in (self.path, self.filename):
            if candidate is None:
                continue
            suffix = Path(candidate).suffix
            if suffix:
                return suffix.lstrip(".")

        return stage.format or registry.default_format


class MaterializationStagePaths(BaseModel):
    """Stage-level configuration for materialized artefacts."""

    model_config = ConfigDict(extra="forbid")

    format: str | None = None
    directory: str | None = None
    datasets: dict[str, MaterializationDatasetPaths] = Field(default_factory=dict)

    @field_validator("format")
    @classmethod
    def normalise_format(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.strip().lower()


class MaterializationPaths(BaseModel):
    """Registry describing how pipeline artefacts should be materialized."""

    model_config = ConfigDict(extra="forbid")

    root: Path = Field(default=Path("data/output"))
    pipeline_subdir: str | None = None
    default_format: str = Field(default="parquet")
    formats: dict[str, MaterializationFormatConfig] = Field(default_factory=dict)
    stages: dict[str, MaterializationStagePaths] = Field(default_factory=dict)

    @field_validator("default_format")
    @classmethod
    def normalise_default_format(cls, value: str) -> str:
        return value.strip().lower()

    @model_validator(mode="after")
    def normalise_format_registry(self) -> "MaterializationPaths":
        if self.formats:
            normalised = {str(key).strip().lower(): value for key, value in self.formats.items()}
            object.__setattr__(self, "formats", normalised)
        return self

    def extension_for(self, format_name: str) -> str:
        """Return the configured file extension for the given format."""

        key = format_name.strip().lower()
        if key in self.formats:
            return self.formats[key].extension
        return f".{key}"

    def resolve_dataset_path(self, stage: str, dataset: str, format: str) -> Path | None:
        """Resolve a dataset path for the requested stage and format."""

        stage_config = self.stages.get(stage)
        if not stage_config:
            return None

        dataset_config = stage_config.datasets.get(dataset)
        if not dataset_config:
            return None

        return dataset_config.resolve(
            stage=stage_config,
            registry=self,
            dataset=dataset,
            format=format,
        )

    def infer_dataset_format(self, stage: str, dataset: str) -> str | None:
        """Infer preferred materialization format for a dataset."""

        stage_config = self.stages.get(stage)
        if not stage_config:
            return None

        dataset_config = stage_config.datasets.get(dataset)
        if not dataset_config:
            candidate = stage_config.format or self.default_format
            return candidate.strip().lower() if candidate else None

        inferred = dataset_config.infer_default_format(stage=stage_config, registry=self)
        return inferred.strip().lower() if inferred else None

    def _normalise_path(self, candidate: Path | str) -> Path:
        """Normalise relative paths against the configured materialization root."""

        path = Path(candidate)
        if path.is_absolute():
            return path

        base = Path(self.root)
        if self.pipeline_subdir:
            base = base / self.pipeline_subdir

        return (base / path).resolve()


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
