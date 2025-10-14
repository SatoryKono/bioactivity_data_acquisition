"""Configuration management for the bioactivity ETL pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

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


class OutputSettings(BaseModel):
    """Paths for ETL outputs."""

    data_path: Path
    qc_report_path: Path
    correlation_path: Path

    @field_validator("data_path", "qc_report_path", "correlation_path")
    @classmethod
    def ensure_parent_exists(cls, value: Path) -> Path:
        value.parent.mkdir(parents=True, exist_ok=True)
        return value


class LoggingSettings(BaseModel):
    """Structured logging configuration."""

    level: str = Field(default="INFO")


class ValidationSettings(BaseModel):
    """Data validation configuration."""

    strict: bool = Field(default=True)


class Config(BaseModel):
    """Top-level configuration for the ETL pipeline."""

    clients: list[APIClientConfig]
    output: OutputSettings
    retries: RetrySettings = Field(default_factory=RetrySettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    validation: ValidationSettings = Field(default_factory=ValidationSettings)

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
    "LoggingSettings",
    "OutputSettings",
    "RetrySettings",
    "ValidationSettings",
]
