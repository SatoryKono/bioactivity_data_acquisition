"""Configuration management for the bioactivity ETL pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, HttpUrl, field_validator


class RetrySettings(BaseModel):
    """Retry configuration for HTTP clients."""

    max_tries: int = Field(default=5, ge=1)
    backoff_multiplier: float = Field(default=1.0, gt=0)


class APIClientConfig(BaseModel):
    """Configuration for a single HTTP API client."""

    name: str
    url: HttpUrl
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    pagination_param: str | None = None
    page_size_param: str | None = None
    page_size: int | None = Field(default=None, gt=0)
    max_pages: int | None = Field(default=None, gt=0)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Client name must not be empty")
        return value


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
    def ensure_parent_exists(cls, value: Path) -> Path:
        value.parent.mkdir(parents=True, exist_ok=True)
        return value


class IOSettings(BaseModel):
    """I/O configuration namespace."""

    output: OutputSettings


class LoggingSettings(BaseModel):
    """Structured logging configuration."""

    level: str = Field(default="INFO")


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

    by: list[str] = Field(default_factory=lambda: ["compound_id", "target"])
    ascending: list[bool] | bool = Field(default=True)
    na_position: Literal["first", "last"] = Field(default="last")


class DeterminismSettings(BaseModel):
    """Deterministic ordering configuration."""

    sort: SortSettings = Field(default_factory=SortSettings)
    column_order: list[str] = Field(
        default_factory=lambda: [
            "compound_id",
            "target",
            "activity_value",
            "activity_unit",
            "source",
            "retrieved_at",
            "smiles",
        ]
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


class CorrelationSettings(BaseModel):
    """Configuration for correlation matrix generation."""

    enabled: bool = Field(default=True)


class PostprocessSettings(BaseModel):
    """Post-processing configuration."""

    qc: QCStepSettings = Field(default_factory=QCStepSettings)
    correlation: CorrelationSettings = Field(default_factory=CorrelationSettings)


class Config(BaseModel):
    """Top-level configuration for the ETL pipeline."""

    clients: list[APIClientConfig]
    io: IOSettings
    retries: RetrySettings = Field(default_factory=RetrySettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    validation: ValidationSettings = Field(default_factory=ValidationSettings)
    determinism: DeterminismSettings = Field(default_factory=DeterminismSettings)
    transforms: TransformSettings = Field(default_factory=TransformSettings)
    postprocess: PostprocessSettings = Field(default_factory=PostprocessSettings)

    @classmethod
    def load(cls, path: Path | str) -> Config:
        """Load configuration from a YAML file."""

        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        with config_path.open("r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
        return cls.model_validate(data)


__all__ = [
    "APIClientConfig",
    "Config",
    "CorrelationSettings",
    "CsvFormatSettings",
    "DeterminismSettings",
    "IOSettings",
    "LoggingSettings",
    "OutputSettings",
    "ParquetFormatSettings",
    "PostprocessSettings",
    "QCStepSettings",
    "QCValidationSettings",
    "RetrySettings",
    "SortSettings",
    "TransformSettings",
    "ValidationSettings",
]
