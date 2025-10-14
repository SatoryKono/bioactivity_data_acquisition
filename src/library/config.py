"""Configuration loading utilities."""
from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class RetryConfig(BaseModel):
    """Retry behaviour configuration."""

    max_tries: int = Field(default=5, ge=1)
    max_time: float | None = Field(default=None, ge=0)


class SourceConfig(BaseModel):
    """Configuration for an upstream data source."""

    name: str
    base_url: HttpUrl
    activities_endpoint: str
    page_size: int = Field(default=200, ge=1)
    auth_token: str | None = None


class OutputConfig(BaseModel):
    """Configuration for output artefacts."""

    output_path: Path
    qc_report_path: Path
    correlation_path: Path


class PipelineConfig(BaseSettings):
    """Top level configuration object for the pipeline."""

    sources: list[SourceConfig]
    output: OutputConfig
    retries: RetryConfig = Field(default_factory=RetryConfig)
    log_level: str = Field(default="INFO")
    strict_validation: bool = Field(default=True)

    model_config = SettingsConfigDict(env_prefix="BIOACTIVITY_")

    @classmethod
    def from_file(cls, path: Path, *, env_file: Path | None = None) -> PipelineConfig:
        """Load configuration from a YAML file and optional ``.env`` file."""
        data = load_yaml(path)
        if env_file is not None and env_file.exists():
            env_mapping = load_env(env_file)
            sources = data.get("sources", [])
            for source in sources:
                name = source.get("name")
                if not isinstance(name, str):
                    continue
                env_key = f"{name.upper()}_AUTH_TOKEN"
                token = env_mapping.get(env_key)
                if token:
                    source["auth_token"] = token
        settings_kwargs: dict[str, Any] = {}
        if env_file is not None and env_file.exists():
            settings_kwargs["_env_file"] = env_file
            settings_kwargs["_env_file_encoding"] = "utf-8"
        return cls(**data, **settings_kwargs)


def load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML file from ``path``."""
    with path.open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def load_env(path: Path) -> Mapping[str, str]:
    """Load environment variables from ``path`` using ``python-dotenv``."""
    return {key: value for key, value in dotenv_values(path).items() if value is not None}
