"""Configuration management for testitem ETL pipeline."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from library.config import Config


class ConfigLoadError(RuntimeError):
    """Raised when configuration loading fails."""

    pass


class TestitemRuntimeSettings(BaseModel):
    """Runtime settings for testitem pipeline."""

    date_tag: str | None = Field(default=None)
    workers: int = Field(default=4, ge=1)
    limit: int | None = Field(default=None, ge=1)
    dry_run: bool = Field(default=False)
    batch_size: int = Field(default=50, ge=1, le=200)
    pubchem_batch_size: int = Field(default=100, ge=1, le=200)
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
<<<<<<< Updated upstream
=======

    # Determinism settings
    determinism: DeterminismSettings = Field(default_factory=DeterminismSettings)
>>>>>>> Stashed changes

    @classmethod
    def from_file(cls, path: Path) -> TestitemConfig:
        """Load configuration from YAML file."""
        try:
<<<<<<< Updated upstream
            # Use the base class load method
            config = cls.load(path)
            
=======
            # Load raw YAML data
            with path.open("r", encoding="utf-8") as f:
                raw_data = yaml.safe_load(f) or {}

            # Process environment placeholders
            processed_data = cls._process_env_placeholders(raw_data)

            # Extract determinism settings if present
            determinism_data = processed_data.pop("determinism", {})

            # Use the base class load method with processed data
            config = cls.model_validate(processed_data)

            # Set determinism settings if provided
            if determinism_data:
                config.determinism = DeterminismSettings.model_validate(determinism_data)

>>>>>>> Stashed changes
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

                return re.sub(r"\{([^}]+)\}", replace_func, value)
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
        if hasattr(self, "output") and hasattr(self.output, "dir"):
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
        if hasattr(self, "output") and hasattr(self.output, "csv_pattern"):
            return self.output.csv_pattern.format(run_date=run_date)
        return f"testitem_{run_date}.csv"

    def get_meta_filename(self, run_date: str) -> str:
        """Get metadata filename."""
        if hasattr(self, "output") and hasattr(self.output, "meta_filename"):
            return self.output.meta_filename.format(run_date=run_date)
        return f"testitem_{run_date}_meta.yaml"

    def get_qc_dir(self) -> Path:
        """Get QC directory path."""
        if hasattr(self, "output") and hasattr(self.output, "qc_dir"):
            return Path(self.output.qc_dir)
        return Path("data/output/testitem")

    def get_logs_dir(self) -> Path:
        """Get logs directory path."""
        if hasattr(self, "output") and hasattr(self.output, "logs_dir"):
            return Path(self.output.logs_dir)
        return Path("logs")
