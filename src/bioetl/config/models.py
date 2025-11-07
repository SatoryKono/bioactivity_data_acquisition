"""Pydantic models describing the pipeline configuration schema."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, PositiveFloat, PositiveInt, model_validator

StatusCode = Annotated[int, Field(ge=100, le=599)]


class RetryConfig(BaseModel):
    """Retry policy for HTTP clients."""

    model_config = ConfigDict(extra="forbid")

    total: PositiveInt = Field(
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


class CacheConfig(BaseModel):
    """Configuration for the HTTP cache layer."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="Enable or disable the on-disk cache.")
    directory: str = Field(default="http_cache", description="Directory used to store cached data.")
    ttl: PositiveInt = Field(
        default=86_400, description="Time-to-live for cached entries in seconds."
    )


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


class PipelineMetadata(BaseModel):
    """Descriptive metadata for the pipeline itself."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Unique name of the pipeline (e.g., activity, assay).")
    version: str = Field(..., description="Semantic version of the pipeline implementation.")
    owner: str | None = Field(
        default=None, description="Team or individual responsible for the pipeline."
    )
    description: str | None = Field(default=None, description="Short human readable description.")


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
    http: HTTPConfig = Field(..., description="HTTP client defaults and profiles.")
    cache: CacheConfig = Field(default_factory=CacheConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)
    determinism: DeterminismConfig = Field(default_factory=DeterminismConfig)
    materialization: MaterializationConfig = Field(default_factory=MaterializationConfig)
    fallbacks: FallbacksConfig = Field(default_factory=FallbacksConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    transform: TransformConfig = Field(default_factory=TransformConfig)
    postprocess: PostprocessConfig = Field(default_factory=PostprocessConfig)
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
