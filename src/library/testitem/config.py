"""Configuration management for testitem ETL pipeline."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from library.config import Config
from pydantic import BaseModel, Field


class ConfigLoadError(RuntimeError):
    """Raised when configuration loading fails."""
    pass


class TestitemRuntimeSettings(BaseModel):
    """Runtime settings for testitem pipeline."""
    
    date_tag: str | None = Field(default=None)
    workers: int = Field(default=4, ge=1)
    limit: int | None = Field(default=None, ge=1)
    dry_run: bool = Field(default=False)
    batch_size: int = Field(default=200, ge=1)
    retries: int = Field(default=5, ge=1)
    timeout_sec: int = Field(default=30, ge=1)
    cache_dir: str = Field(default=".cache/chembl")
    pubchem_cache_dir: str = Field(default=".cache/pubchem")


class TestitemConfig(Config):
    """Configuration for testitem ETL pipeline."""

    # Pipeline-specific settings
    pipeline_version: str = "1.1.0"
    allow_parent_missing: bool = False
    enable_pubchem: bool = True
    
    # Runtime settings
    runtime: TestitemRuntimeSettings = Field(default_factory=TestitemRuntimeSettings)

    @classmethod
    def from_file(cls, path: Path) -> TestitemConfig:
        """Load configuration from YAML file."""
        try:
            # Use the base class load method
            config = cls.load(path)
            
            # Validate testitem-specific configuration
            config._validate()
            
            return config
            
        except Exception as exc:
            raise ConfigLoadError(f"Failed to load configuration from {path}: {exc}") from exc

    @staticmethod
    def _process_env_placeholders(data: dict[str, Any]) -> dict[str, Any]:
        """Process environment variable placeholders in configuration data."""
        import re
        
        def replace_placeholder(value: Any) -> Any:
            if isinstance(value, str):
                # Replace {VAR_NAME} with environment variable value
                def replace_func(match):
                    env_var = match.group(1)
                    return os.environ.get(env_var, match.group(0))
                return re.sub(r'\{([^}]+)\}', replace_func, value)
            elif isinstance(value, dict):
                return {k: replace_placeholder(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [replace_placeholder(item) for item in value]
            else:
                return value
        
        return replace_placeholder(data)

    def _validate(self) -> None:
        """Validate configuration settings."""
        # Validate required directories
        if hasattr(self, 'output') and hasattr(self.output, 'dir'):
            output_dir = Path(self.output.dir)
            if not output_dir.parent.exists():
                raise ConfigLoadError(f"Output directory parent does not exist: {output_dir.parent}")
        
        # Validate pipeline version format
        if not self.pipeline_version or not isinstance(self.pipeline_version, str):
            raise ConfigLoadError("pipeline_version must be a non-empty string")
        
        # Validate boolean flags
        if not isinstance(self.allow_parent_missing, bool):
            raise ConfigLoadError("allow_parent_missing must be a boolean")
        
        if not isinstance(self.enable_pubchem, bool):
            raise ConfigLoadError("enable_pubchem must be a boolean")

    def get_output_filename(self, run_date: str) -> str:
        """Generate output filename based on run date."""
        if hasattr(self, 'output') and hasattr(self.output, 'csv_pattern'):
            return self.output.csv_pattern.format(run_date=run_date)
        return f"testitem__{run_date}.csv"

    def get_meta_filename(self) -> str:
        """Get metadata filename."""
        if hasattr(self, 'output') and hasattr(self.output, 'meta_filename'):
            return self.output.meta_filename
        return "meta.yaml"

    def get_qc_dir(self) -> Path:
        """Get QC directory path."""
        if hasattr(self, 'output') and hasattr(self.output, 'qc_dir'):
            return Path(self.output.qc_dir)
        return Path("data/qc/testitem")

    def get_logs_dir(self) -> Path:
        """Get logs directory path."""
        if hasattr(self, 'output') and hasattr(self.output, 'logs_dir'):
            return Path(self.output.logs_dir)
        return Path("logs")
