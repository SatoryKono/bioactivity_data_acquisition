"""Configuration models and loader for the testitem ETL pipeline (documents-style)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from library.config import (
    _assign_path,
    _merge_dicts,
    _parse_scalar,
    DeterminismSettings,
    LoggingSettings,
)


ALLOWED_SOURCES: tuple[str, ...] = ("chembl", "pubchem")
DATE_TAG_FORMAT = "%Y%m%d"
DATE_TAG_REGEX = r"^\d{8}$"
DEFAULT_ENV_PREFIX = "BIOACTIVITY__"


class TestitemInputSettings(BaseModel):
    """Input configuration for the testitem pipeline."""

    testitems_csv: Path = Field(default=Path("data/input/testitems.csv"))


class TestitemOutputSettings(BaseModel):
    """Output configuration for generated artefacts (documents-style)."""

    dir: Path = Field(default=Path("data/output/testitem"))
    # Backward-compat optional patterns (if provided in configs)
    model_config = ConfigDict(extra="ignore")
    csv_pattern: str | None = Field(default=None)
    meta_filename: str | None = Field(default=None)
    qc_dir: str | None = Field(default=None)
    logs_dir: str | None = Field(default=None)


class TestitemIOSettings(BaseModel):
    """Container for I/O configuration."""

    input: TestitemInputSettings = Field(default_factory=TestitemInputSettings)
    output: TestitemOutputSettings = Field(default_factory=TestitemOutputSettings)


class TestitemRuntimeSettings(BaseModel):
    """Runtime toggles controlled via CLI or environment variables."""

    date_tag: str | None = Field(default=None, pattern=DATE_TAG_REGEX)
    workers: int = Field(default=4, ge=1, le=64)
    limit: int | None = Field(default=None, ge=1)
    dry_run: bool = Field(default=False)
    # Keep testitem-specific knobs for compatibility
    batch_size: int = Field(default=200, ge=1)
    retries: int = Field(default=5, ge=1)
    timeout_sec: int = Field(default=30, ge=1)
    cache_dir: str = Field(default=".cache/chembl")
    pubchem_cache_dir: str = Field(default=".cache/pubchem")


class TestitemHTTPRetrySettings(BaseModel):
    """Retry configuration for HTTP calls."""

    total: int = Field(default=5, ge=0, le=10)
    backoff_multiplier: float = Field(default=2.0, gt=0.0, le=10.0)


class TestitemHTTPGlobalSettings(BaseModel):
    """Global HTTP behaviour applied to all sources (documents-style: no rate_limit here)."""

    model_config = ConfigDict(extra="ignore")
    timeout_sec: float = Field(default=30.0, gt=0.0, le=600.0)
    retries: TestitemHTTPRetrySettings = Field(default_factory=TestitemHTTPRetrySettings)
    headers: dict[str, str] = Field(default_factory=dict)


class TestitemHTTPSettings(BaseModel):
    """HTTP configuration namespace."""

    model_config = ConfigDict(populate_by_name=True)
    global_: TestitemHTTPGlobalSettings = Field(default_factory=TestitemHTTPGlobalSettings, alias="global")


class SourceToggle(BaseModel):
    """Enable or disable a specific feature/step."""

    enabled: bool = Field(default=True)


class TestitemSourceHTTPSettings(BaseModel):
    """HTTP settings specific to a testitem source."""

    model_config = ConfigDict(extra="ignore")
    base_url: str | None = Field(default=None)
    timeout_sec: float | None = Field(default=None, gt=0.0, le=600.0)
    headers: dict[str, str] = Field(default_factory=dict)
    retries: dict[str, Any] = Field(default_factory=dict)


class TestitemSourcePaginationSettings(BaseModel):
    """Pagination settings for a testitem source (documents-style)."""

    model_config = ConfigDict(extra="ignore")
    page_param: str | None = Field(default=None)
    size_param: str | None = Field(default=None)
    size: int | None = Field(default=None, ge=1, le=1000)
    max_pages: int | None = Field(default=None, ge=1, le=1000)


class TestitemSourceSettings(BaseModel):
    """Configuration settings for a testitem source."""

    model_config = ConfigDict(extra="ignore")
    enabled: bool = Field(default=True)
    name: str | None = Field(default=None)
    endpoint: str | None = Field(default=None)
    params: dict[str, Any] = Field(default_factory=dict)
    pagination: TestitemSourcePaginationSettings = Field(default_factory=TestitemSourcePaginationSettings)
    http: TestitemSourceHTTPSettings = Field(default_factory=TestitemSourceHTTPSettings)
    rate_limit: dict[str, Any] = Field(default_factory=dict)


class TestitemPostprocessSettings(BaseModel):
    """Postprocessing configuration for testitems."""

    qc: SourceToggle = Field(default_factory=lambda: SourceToggle(enabled=True))
    correlation: SourceToggle = Field(default_factory=lambda: SourceToggle(enabled=False))


def _default_sources() -> dict[str, TestitemSourceSettings]:
    return {name: TestitemSourceSettings(name=name) for name in ALLOWED_SOURCES}


class TestitemConfig(BaseModel):
    """Top-level configuration for the testitem ETL pipeline (documents-style)."""

    # Pipeline-specific settings
    pipeline_version: str = Field(default="1.1.0")
    allow_parent_missing: bool = Field(default=False)
    enable_pubchem: bool = Field(default=True)

    # Namespaces
    io: TestitemIOSettings = Field(default_factory=TestitemIOSettings)
    runtime: TestitemRuntimeSettings = Field(default_factory=TestitemRuntimeSettings)
    http: TestitemHTTPSettings = Field(default_factory=TestitemHTTPSettings)
    sources: dict[str, TestitemSourceSettings] = Field(default_factory=_default_sources)
    postprocess: TestitemPostprocessSettings = Field(default_factory=TestitemPostprocessSettings)
    determinism: DeterminismSettings = Field(default_factory=DeterminismSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    model_config = ConfigDict(extra="ignore")

    @field_validator("sources", mode="before")
    @classmethod
    def _normalise_sources(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {name: {"enabled": True, "name": name} for name in ALLOWED_SOURCES}
        if isinstance(value, Mapping):
            normalised: dict[str, Any] = {}
            for raw_key, raw_value in value.items():
                key = str(raw_key).lower()
                if key not in ALLOWED_SOURCES:
                    raise ValueError(f"Unsupported source '{raw_key}'")
                if isinstance(raw_value, Mapping):
                    source_config = dict(raw_value)
                    if "name" not in source_config:
                        source_config["name"] = key
                    normalised[key] = source_config
                elif isinstance(raw_value, bool):
                    normalised[key] = {"enabled": raw_value, "name": key}
                else:
                    raise ValueError(f"Invalid configuration for source '{raw_key}'")
            for name in ALLOWED_SOURCES:
                normalised.setdefault(name, {"enabled": True, "name": name})
            return normalised
        raise ValueError("sources must be a mapping of source configurations")

    @model_validator(mode="after")
    def _validate_sources(self) -> "TestitemConfig":
        if not any(toggle.enabled for toggle in self.sources.values()):
            raise ValueError("At least one source must be enabled")
        # Basic checks for pipeline version
        if not self.pipeline_version or not isinstance(self.pipeline_version, str):
            raise ValueError("pipeline_version must be a non-empty string")
        return self

    def enabled_sources(self) -> list[str]:
        """Return the list of enabled sources respecting declaration order."""

        return [name for name in ALLOWED_SOURCES if self.sources.get(name, TestitemSourceSettings()).enabled]

    # Backward-compat helpers (used by current persist implementation)
    def get_output_filename(self, run_date: str) -> str:
        pattern = getattr(self.io.output, "csv_pattern", None)
        return pattern.format(run_date=run_date) if pattern else f"testitem_{run_date}.csv"

    def get_meta_filename(self, run_date: str) -> str:
        meta = getattr(self.io.output, "meta_filename", None)
        return meta.format(run_date=run_date) if meta else f"testitem_{run_date}_meta.yaml"

    def get_qc_dir(self) -> Path:
        qc_dir = getattr(self.io.output, "qc_dir", None)
        return Path(qc_dir) if qc_dir else self.io.output.dir

    def get_logs_dir(self) -> Path:
        logs_dir = getattr(self.io.output, "logs_dir", None)
        return Path(logs_dir) if logs_dir else Path("logs")


class ConfigLoadError(RuntimeError):
    """Raised when configuration loading fails."""


def _load_env_overrides(prefix: str) -> dict[str, Any]:
    if not prefix:
        return {}
    result: dict[str, Any] = {}
    for key, value in list(os.environ.items()):
        if not key.startswith(prefix):
            continue
        suffix = key[len(prefix) :]
        if not suffix:
            continue
        path = [segment.lower() for segment in suffix.split("__") if segment]
        if not path:
            continue
        _assign_path(result, path, _parse_scalar(value))
    return result


def load_testitem_config(
    config_path: Path | str | None,
    *,
    overrides: Mapping[str, Any] | None = None,
    env_prefix: str = DEFAULT_ENV_PREFIX,
) -> TestitemConfig:
    """Load the testitem configuration applying layered overrides (documents-style)."""

    base_data: dict[str, Any] = {}
    if config_path is not None:
        path = Path(config_path)
        if not path.exists():
            raise ConfigLoadError(f"Configuration file not found: {path}")
        try:
            with path.open("r", encoding="utf-8") as handle:
                base_data = yaml.safe_load(handle) or {}
        except yaml.YAMLError as exc:
            raise ConfigLoadError(f"Failed to parse configuration file: {exc}") from exc
        except OSError as exc:
            raise ConfigLoadError(f"Unable to read configuration file: {exc}") from exc

    env_overrides = _load_env_overrides(env_prefix)
    merged = _merge_dicts(base_data, env_overrides)
    if overrides:
        merged = _merge_dicts(merged, overrides)

    try:
        return TestitemConfig.model_validate(merged)
    except ValidationError as exc:
        raise ConfigLoadError(str(exc)) from exc


__all__ = [
    "ALLOWED_SOURCES",
    "ConfigLoadError",
    "DEFAULT_ENV_PREFIX",
    "DATE_TAG_FORMAT",
    "TestitemConfig",
    "TestitemHTTPGlobalSettings",
    "TestitemHTTPRetrySettings",
    "TestitemHTTPSettings",
    "TestitemIOSettings",
    "TestitemInputSettings",
    "TestitemOutputSettings",
    "TestitemRuntimeSettings",
    "TestitemPostprocessSettings",
    "TestitemSourceSettings",
    "TestitemSourceHTTPSettings",
    "TestitemSourcePaginationSettings",
    "SourceToggle",
    "load_testitem_config",
]
