"""Configuration for target ETL pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from library.config import (
    HTTPGlobalSettings,
    HTTPSettings,
    LoggingSettings,
    RetrySettings,
    ValidationSettings,
)


class TargetInputSettings(BaseModel):
    """Input configuration for the target pipeline."""
    
    path: Path = Field(default=Path("data/input/targets.csv"), description="Path to input CSV file")
    encoding: str = Field(default="utf-8", description="File encoding")
    required_columns: list[str] = Field(default_factory=lambda: ["target_chembl_id"], description="Required columns")


class TargetOutputSettings(BaseModel):
    """Output configuration for the target pipeline."""
    
    dir: Path = Field(default=Path("data/output/targets"), description="Output directory")
    format: str = Field(default="csv", description="Output format")
    csv: dict[str, Any] = Field(default_factory=lambda: {
        "encoding": "utf-8",
        "float_format": "%.6f",
        "date_format": "%Y-%m-%dT%H:%M:%SZ",
        "na_rep": "",
        "line_terminator": "\n"
    }, description="CSV format settings")


class TargetIOSettings(BaseModel):
    """Container for I/O configuration."""
    
    input: TargetInputSettings = Field(default_factory=TargetInputSettings)
    output: TargetOutputSettings = Field(default_factory=TargetOutputSettings)


class TargetRuntimeSettings(BaseModel):
    """Runtime settings for target pipeline."""
    
    limit: int | None = Field(None, description="Limit number of targets to process")
    dry_run: bool = Field(False, description="Run without writing output files")
    dev_mode: bool = Field(False, description="Enable development mode")
    allow_incomplete_sources: bool = Field(False, description="Allow incomplete source data")
    date_tag: str | None = Field(None, description="Date tag for output files (YYYYMMDD)")


class TargetConfig(BaseModel):
    """Configuration for target ETL pipeline."""
    
    # Core settings
    http: HTTPSettings = Field(default_factory=HTTPSettings)
    io: TargetIOSettings = Field(default_factory=TargetIOSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    runtime: TargetRuntimeSettings = Field(default_factory=TargetRuntimeSettings)
    validation: ValidationSettings = Field(default_factory=ValidationSettings)
    sources: dict[str, Any] = Field(default_factory=dict, description="Data sources configuration")
    
    @classmethod
    def from_yaml(cls, config_path: Path | str, overrides: dict[str, Any] | None = None) -> TargetConfig:
        """Load configuration from YAML file."""
        import yaml
        
        config_path = Path(config_path)
        if not config_path.exists():
            raise ConfigLoadError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        if overrides:
            # Apply overrides recursively
            def apply_overrides(data: dict, overrides: dict) -> dict:
                for key, value in overrides.items():
                    if key in data and isinstance(data[key], dict) and isinstance(value, dict):
                        data[key] = apply_overrides(data[key], value)
                    else:
                        data[key] = value
                return data
            
            config_data = apply_overrides(config_data, overrides)
        
        try:
            return cls(**config_data)
        except Exception as e:
            raise ConfigLoadError(f"Failed to load configuration: {e}")


class ConfigLoadError(Exception):
    """Error loading configuration."""
    pass


def load_target_config(config_path: Path | str, overrides: dict[str, Any] | None = None) -> TargetConfig:
    """Load target configuration from YAML file."""
    return TargetConfig.from_yaml(config_path, overrides)
