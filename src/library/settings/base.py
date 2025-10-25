"""Base configuration system for the bioactivity data acquisition pipeline.

This module provides a unified configuration system that consolidates
common configuration patterns and reduces duplication across different
entity types and pipeline configurations.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field, field_validator, model_validator

T = TypeVar("T", bound=BaseModel)


@dataclass
class ConfigMetadata:
    """Metadata for configuration values."""

    source: str  # 'yaml', 'env', 'cli', 'default'
    path: str
    value: Any
    overridden: bool = False


class BaseConfigSection(BaseModel):
    """Base class for configuration sections."""

    model_config = {"extra": "ignore", "validate_assignment": True}

    def get_metadata(self) -> dict[str, ConfigMetadata]:
        """Get metadata for all configuration values."""
        metadata = {}
        for field_name, field_info in self.model_fields.items():
            value = getattr(self, field_name, None)
            metadata[field_name] = ConfigMetadata(source="default", path=field_name, value=value)
        return metadata


class HTTPConfigSection(BaseConfigSection):
    """Base HTTP configuration section."""

    base_url: str | None = Field(default=None, description="Base URL for the service")
    timeout: float = Field(default=30.0, ge=1.0, le=300.0, description="Request timeout in seconds")
    retries: int = Field(default=3, ge=0, le=10, description="Number of retry attempts")
    headers: dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    verify_ssl: bool = Field(default=True, description="Verify SSL certificates")
    follow_redirects: bool = Field(default=True, description="Follow HTTP redirects")


class RateLimitConfigSection(BaseConfigSection):
    """Base rate limiting configuration section."""

    requests_per_second: float = Field(default=1.0, ge=0.1, le=100.0, description="Requests per second")
    burst_size: int = Field(default=10, ge=1, le=1000, description="Burst size for rate limiting")
    backoff_multiplier: float = Field(default=2.0, ge=1.0, le=10.0, description="Backoff multiplier")
    backoff_max: float = Field(default=60.0, ge=1.0, le=300.0, description="Maximum backoff time")


class SourceConfigSection(BaseConfigSection):
    """Base configuration for data sources."""

    enabled: bool = Field(default=True, description="Whether this source is enabled")
    name: str | None = Field(default=None, description="Source name")
    http: HTTPConfigSection = Field(default_factory=HTTPConfigSection)
    rate_limit: RateLimitConfigSection = Field(default_factory=RateLimitConfigSection)
    batch_size: int = Field(default=100, ge=1, le=1000, description="Batch size for API requests")

    @field_validator("name", mode="before")
    @classmethod
    def _set_default_name(cls, v: Any, info: Any) -> str:
        """Set default name from field name if not provided."""
        if v is None and hasattr(info, "field_name"):
            return info.field_name
        return v


class IOSection(BaseConfigSection):
    """Base I/O configuration section."""

    input_dir: Path = Field(default=Path("data/input"), description="Input directory")
    output_dir: Path = Field(default=Path("data/output"), description="Output directory")
    cache_dir: Path = Field(default=Path("data/cache"), description="Cache directory")
    logs_dir: Path = Field(default=Path("logs"), description="Logs directory")
    temp_dir: Path = Field(default=Path("data/temp"), description="Temporary directory")

    @field_validator("input_dir", "output_dir", "cache_dir", "logs_dir", "temp_dir", mode="before")
    @classmethod
    def _convert_path(cls, v: Any) -> Path:
        """Convert string to Path."""
        if isinstance(v, str):
            return Path(v)
        return v


class RuntimeSection(BaseConfigSection):
    """Base runtime configuration section."""

    limit: int | None = Field(default=None, ge=1, description="Limit number of records to process")
    allow_incomplete_sources: bool = Field(default=False, description="Allow incomplete data sources")
    parallel_workers: int = Field(default=1, ge=1, le=32, description="Number of parallel workers")
    memory_limit_mb: int | None = Field(default=None, ge=100, description="Memory limit in MB")

    @field_validator("parallel_workers")
    @classmethod
    def _validate_workers(cls, v: int) -> int:
        """Validate number of workers."""
        # Limit to CPU count if not specified
        cpu_count = os.cpu_count() or 1
        if v > cpu_count:
            return cpu_count
        return v


class LoggingSection(BaseConfigSection):
    """Base logging configuration section."""

    level: str = Field(default="INFO", description="Logging level")
    format: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s", description="Log format")
    file_enabled: bool = Field(default=True, description="Enable file logging")
    console_enabled: bool = Field(default=True, description="Enable console logging")
    max_file_size_mb: int = Field(default=10, ge=1, le=1000, description="Maximum log file size in MB")
    backup_count: int = Field(default=5, ge=1, le=100, description="Number of backup log files")

    @field_validator("level")
    @classmethod
    def _validate_level(cls, v: str) -> str:
        """Validate logging level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid logging level: {v}. Must be one of {valid_levels}")
        return v.upper()


class ValidationSection(BaseConfigSection):
    """Base validation configuration section."""

    strict_mode: bool = Field(default=True, description="Enable strict validation mode")
    skip_invalid_records: bool = Field(default=False, description="Skip invalid records instead of failing")
    max_validation_errors: int = Field(default=100, ge=1, le=10000, description="Maximum validation errors to report")
    validate_schemas: bool = Field(default=True, description="Validate data against schemas")


class DeterminismSection(BaseConfigSection):
    """Base determinism configuration section."""

    sort_by: list[str] = Field(default_factory=list, description="Columns to sort by")
    sort_ascending: bool = Field(default=True, description="Sort in ascending order")
    column_order: list[str] = Field(default_factory=list, description="Column order for output")
    lowercase_columns: list[str] = Field(default_factory=list, description="Columns to convert to lowercase")


class TransformSection(BaseConfigSection):
    """Base transformation configuration section."""

    unit_conversion: bool = Field(default=True, description="Enable unit conversion")
    normalize_strings: bool = Field(default=True, description="Normalize string values")
    remove_duplicates: bool = Field(default=True, description="Remove duplicate records")
    fill_missing_values: bool = Field(default=False, description="Fill missing values")


class PostprocessSection(BaseConfigSection):
    """Base postprocessing configuration section."""

    generate_metadata: bool = Field(default=True, description="Generate metadata files")
    create_checksums: bool = Field(default=True, description="Create checksums for output files")
    compress_output: bool = Field(default=False, description="Compress output files")
    cleanup_temp_files: bool = Field(default=True, description="Clean up temporary files")


class CacheSection(BaseConfigSection):
    """Base caching configuration section."""

    enabled: bool = Field(default=True, description="Enable caching")
    strategy: str = Field(default="file", description="Cache strategy: 'memory', 'file', 'hybrid'")
    ttl_seconds: int = Field(default=3600, ge=60, le=86400, description="Cache TTL in seconds")
    max_size_mb: int = Field(default=100, ge=10, le=10000, description="Maximum cache size in MB")

    @field_validator("strategy")
    @classmethod
    def _validate_strategy(cls, v: str) -> str:
        """Validate cache strategy."""
        valid_strategies = ["memory", "file", "hybrid"]
        if v.lower() not in valid_strategies:
            raise ValueError(f"Invalid cache strategy: {v}. Must be one of {valid_strategies}")
        return v.lower()


class BaseConfig(BaseModel, Generic[T]):
    """Base configuration class for all pipeline configurations."""

    model_config = {"extra": "ignore", "validate_assignment": True}

    # Common sections
    io: IOSection = Field(default_factory=IOSection)
    runtime: RuntimeSection = Field(default_factory=RuntimeSection)
    logging: LoggingSection = Field(default_factory=LoggingSection)
    validation: ValidationSection = Field(default_factory=ValidationSection)
    determinism: DeterminismSection = Field(default_factory=DeterminismSection)
    transforms: TransformSection = Field(default_factory=TransformSection)
    postprocess: PostprocessSection = Field(default_factory=PostprocessSection)
    cache: CacheSection = Field(default_factory=CacheSection)

    @model_validator(mode="after")
    def _validate_config(self) -> BaseConfig:
        """Validate configuration after initialization."""
        # Ensure output directory exists
        self.io.output_dir.mkdir(parents=True, exist_ok=True)
        self.io.cache_dir.mkdir(parents=True, exist_ok=True)
        self.io.logs_dir.mkdir(parents=True, exist_ok=True)
        self.io.temp_dir.mkdir(parents=True, exist_ok=True)

        return self

    def get_metadata(self) -> dict[str, ConfigMetadata]:
        """Get metadata for all configuration values."""
        metadata = {}

        # Add common sections
        for section_name in ["io", "runtime", "logging", "validation", "determinism", "transforms", "postprocess", "cache"]:
            section = getattr(self, section_name, None)
            if section and hasattr(section, "get_metadata"):
                section_metadata = section.get_metadata()
                for key, value in section_metadata.items():
                    metadata[f"{section_name}.{key}"] = value

        return metadata

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        return self.model_dump()

    def save_to_yaml(self, path: Path | str) -> None:
        """Save configuration to YAML file."""
        import yaml

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, indent=2)

    @classmethod
    def from_yaml(cls, path: Path | str) -> BaseConfig:
        """Load configuration from YAML file."""
        import yaml

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        return cls.model_validate(data)

    def get_output_path(self) -> Path:
        """Get output path for this configuration."""
        return self.io.output_dir

    def get_cache_path(self) -> Path:
        """Get cache path for this configuration."""
        return self.io.cache_dir

    def get_logs_path(self) -> Path:
        """Get logs path for this configuration."""
        return self.io.logs_dir


class ConfigFactory(ABC):
    """Factory for creating configuration instances."""

    @abstractmethod
    def create_config(self, data: dict[str, Any]) -> BaseConfig:
        """Create configuration instance from data."""
        pass

    @abstractmethod
    def get_default_config(self) -> BaseConfig:
        """Get default configuration."""
        pass

    @abstractmethod
    def get_config_schema(self) -> dict[str, Any]:
        """Get configuration schema."""
        pass


class ConfigValidator:
    """Validator for configuration values."""

    def __init__(self, config: BaseConfig):
        self.config = config

    def validate(self) -> list[str]:
        """Validate configuration and return list of errors."""
        errors = []

        # Validate paths
        if not self.config.io.input_dir.exists():
            errors.append(f"Input directory does not exist: {self.config.io.input_dir}")

        # Validate runtime settings
        if self.config.runtime.limit is not None and self.config.runtime.limit < 1:
            errors.append("Runtime limit must be at least 1")

        # Validate logging settings
        if self.config.logging.max_file_size_mb < 1:
            errors.append("Log file size must be at least 1 MB")

        # Validate cache settings
        if self.config.cache.enabled and self.config.cache.max_size_mb < 10:
            errors.append("Cache size must be at least 10 MB")

        return errors

    def is_valid(self) -> bool:
        """Check if configuration is valid."""
        return len(self.validate()) == 0


class ConfigMerger:
    """Utility for merging configurations."""

    @staticmethod
    def merge_configs(base: BaseConfig, override: BaseConfig) -> BaseConfig:
        """Merge two configurations, with override taking precedence."""
        base_dict = base.to_dict()
        override_dict = override.to_dict()

        # Deep merge dictionaries
        merged_dict = ConfigMerger._deep_merge(base_dict, override_dict)

        # Create new instance
        return base.__class__.model_validate(merged_dict)

    @staticmethod
    def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = ConfigMerger._deep_merge(result[key], value)
            else:
                result[key] = value

        return result


class ConfigLoader:
    """Utility for loading configurations from various sources."""

    @staticmethod
    def load_from_file(path: Path | str) -> dict[str, Any]:
        """Load configuration from file."""
        path = Path(path)

        if path.suffix.lower() == ".yaml" or path.suffix.lower() == ".yml":
            import yaml

            with open(path) as f:
                return yaml.safe_load(f)
        elif path.suffix.lower() == ".json":
            import json

            with open(path) as f:
                return json.load(f)
        else:
            raise ValueError(f"Unsupported configuration file format: {path.suffix}")

    @staticmethod
    def load_from_env(prefix: str = "BIOACTIVITY_") -> dict[str, Any]:
        """Load configuration from environment variables."""
        config = {}

        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Convert ENV_VAR to nested dict structure
                config_key = key[len(prefix) :].lower().replace("_", ".")
                ConfigLoader._set_nested_value(config, config_key, value)

        return config

    @staticmethod
    def _set_nested_value(d: dict[str, Any], key: str, value: Any) -> None:
        """Set nested dictionary value from dot-separated key."""
        keys = key.split(".")
        current = d

        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]

        current[keys[-1]] = value
