"""Unified pipeline configuration models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveFloat,
    PositiveInt,
    field_validator,
    model_validator,
)

from .policies import (
    DeterminismConfig,
    FallbacksConfig,
    HTTPClientConfig,
    HTTPConfig,
)


class PipelineMetadata(BaseModel):
    """Descriptive metadata for the pipeline itself."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Unique name of the pipeline (e.g., activity, assay).")
    version: str = Field(..., description="Semantic version of the pipeline implementation.")
    owner: str | None = Field(
        default=None, description="Team or individual responsible for the pipeline."
    )
    description: str | None = Field(default=None, description="Short human readable description.")


class RuntimeConfig(BaseModel):
    """Controls execution-level parameters shared across pipelines."""

    model_config = ConfigDict(extra="forbid")

    parallelism: PositiveInt = Field(
        default=4,
        description="Количество параллельных воркеров для этапов extract/transform.",
    )
    chunk_rows: PositiveInt = Field(
        default=100_000,
        description="Размер чанка строк для пакетной обработки источников.",
    )
    dry_run: bool = Field(
        default=False,
        description="Режим проверки без записи артефактов во внешние системы.",
    )
    seed: int = Field(
        default=42,
        description="Инициализационное значение генераторов случайных чисел для детерминизма.",
    )


class CacheConfig(BaseModel):
    """Configuration for the HTTP cache layer."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="Enable or disable the on-disk cache.")
    directory: str = Field(default="http_cache", description="Directory used to store cached data.")
    ttl: PositiveInt = Field(
        default=86_400, description="Time-to-live for cached entries in seconds."
    )


class IOInputConfig(BaseModel):
    """Настройки чтения входных ресурсов."""

    model_config = ConfigDict(extra="forbid")

    format: str = Field(default="csv", description="Формат входного файла (csv, parquet, json).")
    encoding: str = Field(default="utf-8", description="Кодировка входных файлов.")
    header: bool = Field(default=True, description="Наличие строки заголовков во входном файле.")
    path: str | None = Field(
        default=None,
        description="Явный путь к локальному входному файлу, если он задан.",
    )


class IOOutputConfig(BaseModel):
    """Настройки сериализации итоговых артефактов."""

    model_config = ConfigDict(extra="forbid")

    format: str = Field(default="parquet", description="Формат итоговых данных (parquet, csv).")
    partition_by: Sequence[str] = Field(
        default_factory=tuple,
        description="Список колонок для партиционирования набора данных.",
    )
    overwrite: bool = Field(
        default=True,
        description="Разрешить перезапись ранее существующих артефактов.",
    )
    path: str | None = Field(
        default=None,
        description="Явный путь к выходному файлу или директории.",
    )


class IOConfig(BaseModel):
    """Единая конфигурация ввода/вывода для пайплайна."""

    model_config = ConfigDict(extra="forbid")

    input: IOInputConfig = Field(default_factory=IOInputConfig)
    output: IOOutputConfig = Field(default_factory=IOOutputConfig)


class PathsConfig(BaseModel):
    """Paths used by the pipeline runtime."""

    model_config = ConfigDict(extra="forbid")

    input_root: str = Field(default="data/input", description="Default directory for input assets.")
    output_root: str = Field(
        default="data/output", description="Default directory for pipeline outputs."
    )
    samples_root: str = Field(
        default="data/samples",
        description="Directory containing lightweight sample artifacts for local development.",
    )
    remote_output_root: str | None = Field(
        default=None,
        description="External object storage location for production-scale outputs (e.g., S3 URI).",
    )
    cache_root: str = Field(
        default=".cache", description="Root directory for transient cache files."
    )


class MaterializationConfig(BaseModel):
    """Settings controlling how artifacts are written to disk."""

    model_config = ConfigDict(extra="forbid")

    root: str = Field(
        default="data/output", description="Base directory for materialized datasets."
    )
    default_format: str = Field(
        default="parquet",
        description="Default output format for tabular data (e.g., parquet, csv).",
    )
    pipeline_subdir: str | None = Field(
        default=None,
        description="Optional subdirectory under the output root for this pipeline run.",
    )
    filename_template: str | None = Field(
        default=None,
        description="Optional template for dataset filenames (supports Jinja-style placeholders).",
    )


class ValidationConfig(BaseModel):
    """Pandera schema configuration."""

    model_config = ConfigDict(extra="allow")

    schema_in: str | None = Field(
        default=None,
        description="Dotted path to the Pandera schema validating extracted data.",
    )
    schema_out: str | None = Field(
        default=None,
        description="Dotted path to the Pandera schema validating transformed data.",
    )
    strict: bool = Field(
        default=True, description="If true, Pandera enforces column order and presence."
    )
    coerce: bool = Field(
        default=True, description="If true, Pandera coerces data types during validation."
    )


class TransformConfig(BaseModel):
    """Configuration for transform operations."""

    model_config = ConfigDict(extra="forbid")

    arrays_to_header_rows: Sequence[str] = Field(
        default_factory=tuple,
        description="List of column names to serialize from array-of-objects to header+rows format.",
    )
    enable_flatten: bool = Field(
        default=True,
        description="If true, enable flattening of nested objects into flat columns with prefixes.",
    )
    enable_serialization: bool = Field(
        default=True,
        description="If true, enable serialization of arrays to pipe-delimited or header+rows format.",
    )
    arrays_simple_to_pipe: Sequence[str] = Field(
        default_factory=tuple,
        description="List of column names containing simple arrays to serialize to pipe-delimited format.",
    )
    arrays_objects_to_header_rows: Sequence[str] = Field(
        default_factory=tuple,
        description="List of column names containing arrays of objects to serialize to header+rows format.",
    )
    flatten_objects: Mapping[str, Sequence[str]] = Field(
        default_factory=dict,
        description="Mapping of nested object column names to lists of field names to flatten.",
    )


class PostprocessCorrelationConfig(BaseModel):
    """Post-processing options for correlation analysis."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False,
        description="If true, emit correlation reports alongside the dataset.",
    )


class PostprocessConfig(BaseModel):
    """Top-level post-processing configuration."""

    model_config = ConfigDict(extra="forbid")

    correlation: PostprocessCorrelationConfig = Field(
        default_factory=PostprocessCorrelationConfig,
        description="Correlation report controls.",
    )


class LoggingConfig(BaseModel):
    """Определяет параметры структурного логирования."""

    model_config = ConfigDict(extra="forbid")

    level: str = Field(default="INFO", description="Уровень логирования UnifiedLogger.")
    format: str = Field(
        default="json",
        description="Формат логов (json, console).",
    )
    with_timestamps: bool = Field(
        default=True,
        description="Включать ли UTC временные метки в вывод логов.",
    )
    context_fields: tuple[str, ...] = Field(
        default_factory=lambda: ("pipeline", "run_id"),
        description="Обязательные поля контекста, которые должны присутствовать в каждом сообщении.",
    )


class TelemetryConfig(BaseModel):
    """Настройки экспорта метрик и трейсов (OpenTelemetry)."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=False, description="Включить экспорт телеметрии.")
    exporter: str | None = Field(
        default=None,
        description="Тип экспортера (jaeger, otlp, console).",
    )
    endpoint: str | None = Field(
        default=None,
        description="URL или адрес экспортера, если требуется.",
    )
    sampling_ratio: PositiveFloat = Field(
        default=1.0,
        description="Доля выборки трасс (1.0 = 100%).",
    )


class CLIConfig(BaseModel):
    """Runtime overrides captured from the CLI layer."""

    model_config = ConfigDict(extra="forbid")

    profiles: Sequence[str] = Field(
        default_factory=tuple,
        description="Profiles requested via the --profile flag (in order).",
    )
    dry_run: bool = Field(
        default=False, description="If true, skip the write/materialization stage."
    )
    limit: PositiveInt | None = Field(
        default=None,
        description="Optional limit applied to extracted records for sampling/testing.",
    )
    sample: PositiveInt | None = Field(
        default=None,
        description="Random sample size requested via the CLI.",
    )
    extended: bool = Field(
        default=False,
        description="If true, enable extended QC artifacts and metrics.",
    )
    date_tag: str | None = Field(
        default=None,
        description="Optional YYYYMMDD tag injected into deterministic artifact names.",
    )
    golden: str | None = Field(
        default=None,
        description="Path to golden dataset for bitwise determinism comparison.",
    )
    verbose: bool = Field(
        default=False,
        description="If true, enable verbose (DEBUG-level) logging output.",
    )
    fail_on_schema_drift: bool = Field(
        default=True,
        description="If true, schema drift raises an error; otherwise it is logged and execution continues.",
    )
    validate_columns: bool = Field(
        default=True,
        description="If true, enforce strict column validation during Pandera checks.",
    )
    input_file: str | None = Field(
        default=None,
        description="Optional path to input file (CSV/Parquet) containing IDs for batch extraction.",
    )
    set_overrides: Mapping[str, Any] = Field(
        default_factory=dict,
        description="Key/value overrides provided via --set CLI arguments.",
    )


class ChemblPreflightRetryConfig(BaseModel):
    """Retry policy for ChEMBL preflight handshake."""

    model_config = ConfigDict(extra="forbid")

    total: int = Field(
        default=3,
        ge=0,
        description="Maximum retry attempts (excluding the first request).",
    )
    backoff_factor: PositiveFloat = Field(
        default=0.5,
        description="Exponential backoff factor between retry attempts.",
    )
    status_forcelist: tuple[int, ...] = Field(
        default=(429, 500, 502, 503, 504),
        description="HTTP status codes that trigger a retry.",
    )
    allowed_methods: tuple[str, ...] = Field(
        default=("GET", "HEAD"),
        description="HTTP methods that may be retried safely.",
    )

    @field_validator("allowed_methods")
    @classmethod
    def _normalize_methods(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized: list[str] = []
        for method in value:
            candidate = method.strip().upper()
            if not candidate:
                continue
            if candidate not in normalized:
                normalized.append(candidate)
        return tuple(normalized)


class ChemblPreflightConfig(BaseModel):
    """Preflight handshake configuration for ChEMBL client."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False,
        description="Enable or disable preflight handshake before pipeline start.",
    )
    url: str = Field(
        default="/data/status",
        description="Primary endpoint used for the preflight handshake.",
    )
    fallback_urls: tuple[str, ...] = Field(
        default=("/data/version", "/data"),
        description="Fallback endpoints probed when the primary handshake fails.",
    )
    retry: ChemblPreflightRetryConfig = Field(
        default_factory=ChemblPreflightRetryConfig,
        description="Retry policy applied to preflight handshake requests.",
    )
    budget_seconds: PositiveFloat = Field(
        default=10.0,
        description="Global wall-clock budget for all preflight attempts.",
    )
    cache_ttl_seconds: PositiveFloat = Field(
        default=600.0,
        description="TTL for successful handshake payload cached in memory.",
    )

    @field_validator("fallback_urls")
    @classmethod
    def _normalize_urls(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        ordered: dict[str, None] = {}
        for candidate in value:
            normalized = candidate.strip()
            if not normalized:
                continue
            if not normalized.startswith("/"):
                normalized = f"/{normalized}"
            ordered.setdefault(normalized, None)
        return tuple(ordered.keys())


class ChemblTimeoutConfig(BaseModel):
    """Timeout overrides for ChEMBL client operations."""

    model_config = ConfigDict(extra="forbid")

    connect_seconds: PositiveFloat = Field(
        default=2.0,
        description="Connection timeout applied to preflight requests.",
    )
    read_seconds: PositiveFloat = Field(
        default=5.0,
        description="Read timeout applied to preflight requests.",
    )
    total_seconds: PositiveFloat = Field(
        default=10.0,
        description="Overall timeout budget for preflight requests.",
    )


class ChemblCircuitBreakerConfig(BaseModel):
    """Circuit breaker policy used by the ChEMBL client."""

    model_config = ConfigDict(extra="forbid")

    error_threshold: PositiveInt = Field(
        default=3,
        description="Number of consecutive failures required to open the circuit.",
    )
    open_seconds: PositiveFloat = Field(
        default=60.0,
        description="Duration in seconds to keep the circuit open before half-open checks.",
    )


class ChemblClientConfig(BaseModel):
    """Composite configuration for the ChEMBL client."""

    model_config = ConfigDict(extra="forbid")

    base_url: str = Field(
        default="https://www.ebi.ac.uk/chembl/api",
        description="Base URL for ChEMBL REST API requests.",
    )
    preflight: ChemblPreflightConfig = Field(
        default_factory=ChemblPreflightConfig,
        description="Preflight handshake settings.",
    )
    timeout: ChemblTimeoutConfig = Field(
        default_factory=ChemblTimeoutConfig,
        description="Timeout overrides for handshake and readiness checks.",
    )
    circuit_breaker: ChemblCircuitBreakerConfig = Field(
        default_factory=ChemblCircuitBreakerConfig,
        description="Circuit breaker policy for outbound calls.",
    )


class ClientsConfig(BaseModel):
    """Client configuration section."""

    model_config = ConfigDict(extra="forbid")

    chembl: ChemblClientConfig = Field(
        default_factory=ChemblClientConfig,
        description="ChEMBL-specific client configuration.",
    )


class SourceConfig(BaseModel):
    """Per-source overrides and metadata."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="Toggle processing of this data source.")
    description: str | None = Field(
        default=None, description="Human readable description of the source."
    )
    http_profile: str | None = Field(
        default=None,
        description="Reference to a named HTTP profile defined under http.profiles.",
    )
    http: HTTPClientConfig | None = Field(
        default=None,
        description="Inline HTTP overrides that take precedence over profile settings.",
    )
    batch_size: PositiveInt | None = Field(
        default=None,
        description="Batch size used when paginating requests for this source.",
    )
    parameters: Mapping[str, Any] = Field(
        default_factory=dict,
        description="Free-form parameters consumed by source-specific components.",
    )


class PipelineConfig(BaseModel):
    """Root configuration object validated at CLI startup."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = Field(
        ..., description="Configuration schema version. Only version=1 is currently supported."
    )
    extends: Sequence[str] = Field(
        default_factory=tuple,
        description="Optional list of profile paths merged before this configuration.",
    )
    pipeline: PipelineMetadata = Field(..., description="Metadata describing the pipeline.")
    runtime: RuntimeConfig = Field(
        default_factory=RuntimeConfig, description="Runtime execution parameters."
    )
    io: IOConfig = Field(
        default_factory=IOConfig, description="Input/output configuration for the pipeline."
    )
    http: HTTPConfig = Field(..., description="HTTP client defaults and profiles.")
    cache: CacheConfig = Field(default_factory=CacheConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)
    determinism: DeterminismConfig = Field(default_factory=DeterminismConfig)
    materialization: MaterializationConfig = Field(default_factory=MaterializationConfig)
    fallbacks: FallbacksConfig = Field(default_factory=FallbacksConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    transform: TransformConfig = Field(default_factory=TransformConfig)
    postprocess: PostprocessConfig = Field(default_factory=PostprocessConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    telemetry: TelemetryConfig = Field(default_factory=TelemetryConfig)
    clients: ClientsConfig = Field(
        default_factory=ClientsConfig,
        description="HTTP client overrides scoped to specific upstream services.",
    )
    sources: dict[str, SourceConfig] = Field(
        default_factory=dict,
        description="Per-source settings keyed by a short identifier.",
    )
    cli: CLIConfig = Field(default_factory=CLIConfig)
    chembl: Mapping[str, Any] | None = Field(
        default=None,
        description="ChEMBL-specific configuration (e.g., enrichment settings).",
    )

    @model_validator(mode="after")
    def ensure_column_order_when_set(self) -> PipelineConfig:
        if self.determinism.column_order and not self.validation.schema_out:
            msg = (
                "determinism.column_order requires validation.schema_out to be set so the schema "
                "can enforce the order"
            )
            raise ValueError(msg)
        return self


__all__ = [
    "PipelineMetadata",
    "RuntimeConfig",
    "CacheConfig",
    "IOInputConfig",
    "IOOutputConfig",
    "IOConfig",
    "PathsConfig",
    "MaterializationConfig",
    "ClientsConfig",
    "ChemblClientConfig",
    "ChemblPreflightConfig",
    "ChemblPreflightRetryConfig",
    "ChemblTimeoutConfig",
    "ChemblCircuitBreakerConfig",
    "ValidationConfig",
    "TransformConfig",
    "PostprocessCorrelationConfig",
    "PostprocessConfig",
    "LoggingConfig",
    "TelemetryConfig",
    "CLIConfig",
    "SourceConfig",
    "PipelineConfig",
]
