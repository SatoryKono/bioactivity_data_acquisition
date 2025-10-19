"""Configuration management for the bioactivity ETL pipeline."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Literal

import jsonschema
import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


def _validate_secrets(headers: dict[str, Any]) -> None:
    """Validate that all required secrets are available in environment variables.

    Checks all header values for placeholder patterns like {API_KEY} and ensures
    the corresponding environment variables are set.

    Args:
        headers: Dictionary of HTTP headers that may contain secret placeholders.

    Raises:
        ValueError: If any required environment variables are missing.
    """
    missing_secrets = []

    for _key, value in headers.items():
        if isinstance(value, str):
            placeholders = re.findall(r"\{([^}]+)\}", value)
            for placeholder in placeholders:
                env_var = os.environ.get(placeholder.upper())
                if env_var is None:
                    missing_secrets.append(placeholder.upper())

    if missing_secrets:
        raise ValueError(f"Missing required environment variables: {', '.join(set(missing_secrets))}. Please set these variables before running the pipeline.")


def _merge_dicts(base: dict[str, Any], overrides: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively merge overrides into base and return a copy.

    Performs deep merge of nested dictionaries, with overrides taking precedence
    over base values.

    Args:
        base: Base dictionary to merge into.
        overrides: Dictionary with override values.

    Returns:
        New dictionary with merged values.
    """

    result = dict(base)
    for key, value in overrides.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            result[key] = _merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def _assign_path(root: dict[str, Any], path: Iterable[str], value: Any) -> None:
    current = root
    *parents, last = list(path)
    for segment in parents:
        current = current.setdefault(segment, {})
    current[last] = value


def _parse_scalar(value: str) -> Any:
    try:
        parsed = yaml.safe_load(value)
    except yaml.YAMLError:
        return value
    return parsed


def ensure_output_directories_exist(config: Config) -> None:
    """Create necessary directories for output files in configuration.

    Creates parent directories for all output paths specified in the configuration
    to ensure they exist before writing files.

    Args:
        config: Configuration object containing output paths.
    """
    output_settings = config.io.output

    # Создаем родительские директории для всех выходных путей
    output_settings.data_path.parent.mkdir(parents=True, exist_ok=True)
    output_settings.qc_report_path.parent.mkdir(parents=True, exist_ok=True)
    output_settings.correlation_path.parent.mkdir(parents=True, exist_ok=True)


class RetrySettings(BaseModel):
    """Retry configuration for HTTP clients."""

    model_config = ConfigDict(populate_by_name=True)

    total: int = Field(default=5, ge=1, alias="max_tries")
    backoff_multiplier: float = Field(default=1.0, gt=0)

    @property
    def max_tries(self) -> int:
        """Backward-compatible accessor for legacy code paths."""

        return self.total


class RateLimitSettings(BaseModel):
    """Rate limiting configuration for API clients."""

    max_calls: int = Field(gt=0)
    period: float = Field(gt=0)


class HTTPGlobalSettings(BaseModel):
    """Defaults applied to all HTTP clients."""

    model_config = ConfigDict(populate_by_name=True)

    timeout_sec: float = Field(default=30.0, gt=0, alias="timeout")
    headers: dict[str, str] = Field(default_factory=dict)
    retries: RetrySettings = Field(default_factory=RetrySettings)
    rate_limit: dict[str, Any] | None = Field(default=None)

    @property
    def timeout(self) -> float:
        """Backward-compatible alias used by existing clients."""

        return self.timeout_sec


class HTTPSourceSettings(BaseModel):
    """Per-source HTTP configuration overriding the global defaults."""

    model_config = ConfigDict(populate_by_name=True)

    base_url: HttpUrl
    timeout_sec: float | None = Field(default=None, gt=0, alias="timeout")
    retries: RetrySettings | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    rate_limit: RateLimitSettings | None = None
    health_endpoint: str | None = Field(default=None, description="Custom health check endpoint. If None, uses base_url for health checks")

    @property
    def timeout(self) -> float | None:
        """Backward-compatible alias for legacy configuration."""

        return self.timeout_sec


class PaginationSettings(BaseModel):
    """Pagination configuration for a data source."""

    page_param: str | None = None
    size_param: str | None = None
    size: int | None = Field(default=None, gt=0)
    max_pages: int | None = Field(default=None, gt=0)


class APIClientConfig(BaseModel):
    """Configuration for a single HTTP API client."""

    name: str
    base_url: HttpUrl
    endpoint: str = ""
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    pagination_param: str | None = None
    page_size_param: str | None = None
    page_size: int | None = Field(default=None, gt=0)
    max_pages: int | None = Field(default=None, gt=0)
    timeout: float = Field(default=30.0, gt=0)
    retries: RetrySettings = Field(default_factory=RetrySettings)
    rate_limit: RateLimitSettings | None = None
    health_endpoint: str | None = Field(default=None, description="Custom health check endpoint. If None, uses base_url for health checks")

    model_config = ConfigDict(frozen=True)

    @property
    def resolved_base_url(self) -> str:
        base = str(self.base_url).rstrip("/")
        if not self.endpoint:
            return base
        endpoint = self.endpoint.lstrip("/")
        return f"{base}/{endpoint}"

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Client name must not be empty")
        return value


class SourceSettings(BaseModel):
    """Configuration for an individual data source."""

    name: str
    enabled: bool = Field(default=True)
    endpoint: str = ""
    params: dict[str, Any] = Field(default_factory=dict)
    pagination: PaginationSettings = Field(default_factory=PaginationSettings)
    http: HTTPSourceSettings

    def to_client_config(self, defaults: HTTPGlobalSettings) -> APIClientConfig:
        # Merge headers and validate secrets
        merged_headers = {**defaults.headers, **self.http.headers}
        _validate_secrets(merged_headers)

        # Process secrets in headers
        processed_headers = {}
        for key, value in merged_headers.items():
            if isinstance(value, str):

                def replace_placeholder(match):
                    secret_name = match.group(1)
                    env_var = os.environ.get(secret_name.upper())
                    return env_var if env_var is not None else match.group(0)

                processed_value = re.sub(r"\{([^}]+)\}", replace_placeholder, value)
                # Only include header if the value is not empty after processing and not a placeholder
                if processed_value and processed_value.strip() and not processed_value.startswith("{") and not processed_value.endswith("}"):
                    processed_headers[key] = processed_value
            else:
                processed_headers[key] = value

        timeout = self.http.timeout_sec or defaults.timeout_sec
        retries = self.http.retries or defaults.retries
        return APIClientConfig(
            name=self.name,
            base_url=self.http.base_url,
            endpoint=self.endpoint,
            headers=processed_headers,
            params=dict(self.params),
            pagination_param=self.pagination.page_param,
            page_size_param=self.pagination.size_param,
            page_size=self.pagination.size,
            max_pages=self.pagination.max_pages,
            timeout=timeout,
            retries=retries,
            rate_limit=self.http.rate_limit,
            health_endpoint=self.http.health_endpoint,
        )


class CsvFormatSettings(BaseModel):
    """Formatting options for CSV outputs."""

    encoding: str = Field(default="utf-8")
    float_format: str | None = Field(default=None)
    date_format: str | None = Field(default=None)
    line_terminator: str | None = Field(default=None)
    na_rep: str | None = Field(default=None)


class ParquetFormatSettings(BaseModel):
    """Formatting options for Parquet outputs."""

    compression: str | None = Field(default="snappy")


class OutputSettings(BaseModel):
    """Paths and formatting for ETL outputs."""

    data_path: Path
    qc_report_path: Path
    correlation_path: Path
    format: Literal["csv", "parquet"] = Field(default="csv")
    csv: CsvFormatSettings = Field(default_factory=CsvFormatSettings)
    parquet: ParquetFormatSettings = Field(default_factory=ParquetFormatSettings)

    @field_validator("data_path", "qc_report_path", "correlation_path")
    @classmethod
    def validate_paths(cls, value: Path) -> Path:
        # Валидация путей без создания директорий
        return value


class InputSettings(BaseModel):
    """Locations of auxiliary input artefacts."""

    documents_csv: Path | None = None


class IOSettings(BaseModel):
    """I/O configuration namespace."""

    input: InputSettings = Field(default_factory=InputSettings)
    output: OutputSettings


class RuntimeSettings(BaseModel):
    """Execution-related toggles exposed to the CLI."""

    workers: int = Field(default=4, ge=1)


class FileLoggingSettings(BaseModel):
    """File logging configuration."""

    enabled: bool = Field(default=True)
    path: Path = Field(default=Path("logs/app.log"))
    max_bytes: int = Field(default=10485760, description="Maximum file size in bytes (10MB)")
    backup_count: int = Field(default=10, description="Number of backup files to keep")
    rotation_strategy: Literal["size", "time"] = Field(default="size", description="Rotation strategy")
    retention_days: int = Field(default=14, description="Days to retain log files")
    cleanup_on_start: bool = Field(default=False, description="Clean old logs on startup")


class ConsoleLoggingSettings(BaseModel):
    """Console logging configuration."""

    format: Literal["text", "json"] = Field(default="text", description="Console output format")


class LoggingSettings(BaseModel):
    """Structured logging configuration."""

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(default="INFO")
    file: FileLoggingSettings = Field(default_factory=FileLoggingSettings)
    console: ConsoleLoggingSettings = Field(default_factory=ConsoleLoggingSettings)


class QCValidationSettings(BaseModel):
    """Thresholds for QC validation."""

    max_missing_fraction: float = Field(default=1.0, ge=0.0, le=1.0)
    max_duplicate_fraction: float = Field(default=1.0, ge=0.0, le=1.0)


class ValidationSettings(BaseModel):
    """Data validation configuration."""

    strict: bool = Field(default=True)
    qc: QCValidationSettings = Field(default_factory=QCValidationSettings)


class SortSettings(BaseModel):
    """Sorting configuration for deterministic outputs."""

    by: list[str] = Field(default_factory=lambda: ["document_chembl_id", "doi"])
    ascending: list[bool] | bool = Field(default=True)
    na_position: Literal["first", "last"] = Field(default="last")


class DeterminismSettings(BaseModel):
    """Deterministic ordering configuration."""

    sort: SortSettings = Field(default_factory=SortSettings)
    column_order: list[str] = Field(
        default_factory=lambda: [
            "document_chembl_id",
            "doi",
            "activity_value",
            "activity_unit",
            "source",
            "retrieved_at",
            "smiles",
        ]
    )
    lowercase_columns: list[str] = Field(
        default_factory=list, description="Список колонок, которые должны быть приведены к нижнему регистру при нормализации. По умолчанию пустой - регистр сохраняется."
    )


class TransformSettings(BaseModel):
    """Transformation configuration for normalization."""

    unit_conversion: dict[str, float] = Field(
        default_factory=lambda: {
            "nM": 1.0,
            "uM": 1000.0,
            "pM": 0.001,
        }
    )


class QCStepSettings(BaseModel):
    """Configuration for QC generation."""

    enabled: bool = Field(default=True)
    enhanced: bool = Field(default=False, description="Enable enhanced QC reporting with detailed metrics")


class CorrelationSettings(BaseModel):
    """Configuration for correlation matrix generation."""

    enabled: bool = Field(default=True)
    enhanced: bool = Field(default=False, description="Enable enhanced correlation analysis with multiple correlation types")


class PostprocessSettings(BaseModel):
    """Post-processing configuration."""

    qc: QCStepSettings = Field(default_factory=QCStepSettings)
    correlation: CorrelationSettings = Field(default_factory=CorrelationSettings)


class HTTPSettings(BaseModel):
    """Container for HTTP configuration sections."""

    model_config = ConfigDict(populate_by_name=True)

    global_: HTTPGlobalSettings = Field(default_factory=HTTPGlobalSettings, alias="global")


class Config(BaseModel):
    """Top-level configuration for the ETL pipeline."""

    http: HTTPSettings
    sources: dict[str, SourceSettings] = Field(default_factory=dict)
    io: IOSettings
    runtime: RuntimeSettings = Field(default_factory=RuntimeSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    validation: ValidationSettings = Field(default_factory=ValidationSettings)
    determinism: DeterminismSettings = Field(default_factory=DeterminismSettings)
    transforms: TransformSettings = Field(default_factory=TransformSettings)
    postprocess: PostprocessSettings = Field(default_factory=PostprocessSettings)

    @classmethod
    def _load_schema(cls) -> dict[str, Any]:
        """Load JSON Schema for configuration validation."""
        schema_path = Path(__file__).parent.parent.parent / "configs" / "schema.json"
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        with schema_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @classmethod
    def _validate_with_schema(cls, config_data: dict[str, Any]) -> None:
        """Validate configuration data against JSON Schema."""
        try:
            schema = cls._load_schema()
            jsonschema.validate(config_data, schema)
        except jsonschema.ValidationError as e:
            raise ValueError(f"Configuration validation failed: {e.message}") from e
        except jsonschema.SchemaError as e:
            raise ValueError(f"Schema error: {e.message}") from e

    @property
    def clients(self) -> list[APIClientConfig]:
        return [source.to_client_config(self.http.global_) for source in self.sources.values()]

    @property
    def output(self) -> OutputSettings:
        return self.io.output

    @property
    def retries(self) -> RetrySettings:
        return self.http.global_.retries

    @classmethod
    def load(
        cls,
        path: Path | str | None,
        *,
        overrides: Mapping[str, Any] | None = None,
        env_prefix: str = "BIOACTIVITY__",
    ) -> Config:
        """Load configuration from a YAML file applying layered overrides."""

        base_data: dict[str, Any] = {}
        if path is not None:
            config_path = Path(path)
            if not config_path.exists():
                raise FileNotFoundError(f"Config file not found: {config_path}")
            with config_path.open("r", encoding="utf-8") as file:
                base_data = yaml.safe_load(file) or {}

        env_overrides = cls._load_env_overrides(env_prefix)
        cli_overrides = cls._normalize_overrides(overrides or {})

        merged = _merge_dicts(base_data, env_overrides)
        merged = _merge_dicts(merged, cli_overrides)

        # Validate against JSON Schema before processing
        cls._validate_with_schema(merged)

        # Process secrets
        merged = cls._process_secrets(merged)

        return cls.model_validate(merged)

    @staticmethod
    def _load_env_overrides(prefix: str) -> dict[str, Any]:
        if not prefix:
            return {}
        result: dict[str, Any] = {}
        for key, value in os.environ.items():
            if not key.startswith(prefix):
                continue
            stripped = key[len(prefix) :]
            if not stripped:
                continue
            path = [segment.lower() for segment in stripped.split("__") if segment]
            if not path:
                continue
            _assign_path(result, path, _parse_scalar(value))
        return result

    @staticmethod
    def _normalize_overrides(overrides: Mapping[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in overrides.items():
            if isinstance(value, Mapping):
                result[key] = Config._normalize_overrides(value)
                continue
            if isinstance(key, str) and "." in key:
                path = [segment.strip().lower() for segment in key.split(".") if segment.strip()]
                if not path:
                    continue
                _assign_path(result, path, value if not isinstance(value, str) else _parse_scalar(value))
            else:
                result[key] = value if not isinstance(value, str) else _parse_scalar(value)
        return result

    @staticmethod
    def parse_cli_overrides(values: list[str]) -> dict[str, str]:
        """Parse CLI override arguments into a dictionary."""
        assignments: dict[str, str] = {}
        for item in values:
            if "=" not in item:
                raise ValueError("Overrides must be in KEY=VALUE format")
            key, value = item.split("=", 1)
            key = key.strip()
            if not key:
                raise ValueError("Override key must not be empty")
            assignments[key] = value
        return assignments

    @staticmethod
    def _process_secrets(config_data: dict[str, Any]) -> dict[str, Any]:
        """Process secret placeholders in configuration data."""
        import re

        def replace_secrets(obj: Any) -> Any:
            if isinstance(obj, str):
                # Replace {secret_name} with environment variable value
                def replace_placeholder(match):
                    secret_name = match.group(1)
                    env_var = os.environ.get(secret_name.upper())
                    return env_var if env_var is not None else match.group(0)

                return re.sub(r"\{([^}]+)\}", replace_placeholder, obj)
            elif isinstance(obj, dict):
                return {key: replace_secrets(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [replace_secrets(item) for item in obj]
            else:
                return obj

        return replace_secrets(config_data)


__all__ = [
    "_assign_path",
    "_merge_dicts",
    "_parse_scalar",
    "ensure_output_directories_exist",
    "APIClientConfig",
    "Config",
    "ConsoleLoggingSettings",
    "CorrelationSettings",
    "CsvFormatSettings",
    "DeterminismSettings",
    "FileLoggingSettings",
    "HTTPGlobalSettings",
    "HTTPSourceSettings",
    "HTTPSettings",
    "IOSettings",
    "InputSettings",
    "LoggingSettings",
    "OutputSettings",
    "PaginationSettings",
    "ParquetFormatSettings",
    "PostprocessSettings",
    "QCStepSettings",
    "QCValidationSettings",
    "RateLimitSettings",
    "RetrySettings",
    "RuntimeSettings",
    "SortSettings",
    "SourceSettings",
    "TransformSettings",
    "ValidationSettings",
]
