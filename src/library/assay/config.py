"""Configuration models and loader for the assay ETL pipeline."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from library.config import _assign_path, _merge_dicts, _parse_scalar, DeterminismSettings, LoggingSettings

ALLOWED_SOURCES: tuple[str, ...] = ("chembl",)
DATE_TAG_FORMAT = "%Y%m%d"
DATE_TAG_REGEX = r"^\d{8}$"
DEFAULT_ENV_PREFIX = "BIOACTIVITY__"


class AssayInputSettings(BaseModel):
    """Input configuration for the assay pipeline."""

    assay_ids_csv: Path = Field(default=Path("data/input/assay_ids.csv"))
    target_ids_csv: Path = Field(default=Path("data/input/target_ids.csv"))


class AssayCSVSettings(BaseModel):
    """CSV output settings."""
    
    encoding: str = Field(default="utf-8")
    float_format: str | None = Field(default="%.3f")
    date_format: str | None = Field(default="%Y-%m-%dT%H:%M:%SZ")
    na_rep: str | None = Field(default="")
    line_terminator: str | None = Field(default=None)


class AssayParquetSettings(BaseModel):
    """Parquet output settings."""
    
    compression: str = Field(default="snappy")


class AssayOutputSettings(BaseModel):
    """Output configuration for generated artefacts."""

    dir: Path = Field(default=Path("data/output/assay"))
    format: str = Field(default="csv")
    csv: AssayCSVSettings = Field(default_factory=AssayCSVSettings)
    parquet: AssayParquetSettings = Field(default_factory=AssayParquetSettings)


class AssayIOSettings(BaseModel):
    """Container for I/O configuration."""

    input: AssayInputSettings = Field(default_factory=AssayInputSettings)
    output: AssayOutputSettings = Field(default_factory=AssayOutputSettings)


class AssayRuntimeSettings(BaseModel):
    """Runtime toggles controlled via CLI or environment variables."""

    date_tag: str | None = Field(
        default=None, pattern=DATE_TAG_REGEX
    )
    workers: int = Field(default=4, ge=1, le=64)
    limit: int | None = Field(default=None, ge=1)
    dry_run: bool = Field(default=False)


class AssayHTTPRetrySettings(BaseModel):
    """Retry configuration for HTTP calls."""

    total: int = Field(default=5, ge=0, le=10)
    backoff_multiplier: float = Field(default=2.0, gt=0.0, le=10.0)


class AssayHTTPGlobalSettings(BaseModel):
    """Global HTTP behaviour applied to all sources."""

    timeout_sec: float = Field(default=30.0, gt=0.0, le=600.0)
    retries: AssayHTTPRetrySettings = Field(default_factory=AssayHTTPRetrySettings)
    headers: dict[str, str] = Field(default_factory=dict)
    rate_limit: dict[str, Any] = Field(default_factory=dict)


class AssayHTTPSettings(BaseModel):
    """HTTP configuration namespace."""

    model_config = ConfigDict(populate_by_name=True)

    global_: AssayHTTPGlobalSettings = Field(
        default_factory=AssayHTTPGlobalSettings, alias="global"
    )


class AssaySourceHTTPSettings(BaseModel):
    """HTTP settings specific to an assay source."""
    
    base_url: str | None = Field(default=None)
    timeout_sec: float | None = Field(default=None, gt=0.0, le=600.0)
    headers: dict[str, str] = Field(default_factory=dict)
    retries: dict[str, Any] = Field(default_factory=dict)


class AssaySourcePaginationSettings(BaseModel):
    """Pagination settings for an assay source."""
    
    page_param: str | None = Field(default=None)
    size_param: str | None = Field(default=None)
    offset_param: str | None = Field(default=None)
    size: int | None = Field(default=None, ge=1, le=1000)
    max_pages: int | None = Field(default=None, ge=1, le=1000)


class AssaySourceSettings(BaseModel):
    """Configuration settings for an assay source."""
    
    enabled: bool = Field(default=True)
    name: str | None = Field(default=None)
    endpoint: str | None = Field(default=None)
    params: dict[str, Any] = Field(default_factory=dict)
    pagination: AssaySourcePaginationSettings = Field(default_factory=AssaySourcePaginationSettings)
    http: AssaySourceHTTPSettings = Field(default_factory=AssaySourceHTTPSettings)


class AssayPostprocessSettings(BaseModel):
    """Postprocessing configuration for assays."""

    qc: SourceToggle = Field(default_factory=lambda: SourceToggle(enabled=True))
    correlation: SourceToggle = Field(default_factory=lambda: SourceToggle(enabled=True))


class SourceToggle(BaseModel):
    """Enable or disable a specific source."""

    enabled: bool = Field(default=True)


class CacheSettings(BaseModel):
    """Cache configuration settings."""
    
    enabled: bool = Field(default=True)
    directory: Path = Field(default=Path(".cache/chembl"))
    ttl: int = Field(default=3600, ge=1, le=86400)  # 1 hour to 1 day
    invalidate_on_release_change: bool = Field(default=True)


class FilterProfileSettings(BaseModel):
    """Filter profile configuration."""
    
    target_organism: str | None = Field(default=None)
    target_type: str | None = Field(default=None)
    relationship_type: str | None = Field(default=None)
    confidence_score__range: str | None = Field(default=None)
    assay_type: str | None = Field(default=None)
    assay_type__in: str | None = Field(default=None)


def _default_sources() -> dict[str, AssaySourceSettings]:
    return {name: AssaySourceSettings(name=name) for name in ALLOWED_SOURCES}


class AssayConfig(BaseModel):
    """Top-level configuration for the assay ETL pipeline."""

    io: AssayIOSettings = Field(default_factory=AssayIOSettings)
    runtime: AssayRuntimeSettings = Field(default_factory=AssayRuntimeSettings)
    http: AssayHTTPSettings = Field(default_factory=AssayHTTPSettings)
    sources: dict[str, AssaySourceSettings] = Field(default_factory=_default_sources)
    postprocess: AssayPostprocessSettings = Field(default_factory=AssayPostprocessSettings)
    determinism: DeterminismSettings = Field(default_factory=DeterminismSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    filter_profiles: dict[str, FilterProfileSettings] = Field(default_factory=dict)

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
                    # Сохраняем все вложенные словари без отбрасывания
                    source_config = dict(raw_value)
                    # Устанавливаем имя источника если не указано
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
    def _validate_sources(self) -> AssayConfig:
        if not any(toggle.enabled for toggle in self.sources.values()):
            raise ValueError("At least one source must be enabled")
        return self

    def enabled_sources(self) -> list[str]:
        """Return the list of enabled sources respecting declaration order."""

        return [name for name in ALLOWED_SOURCES if self.sources.get(name, AssaySourceSettings()).enabled]


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


def load_assay_config(
    config_path: Path | str | None,
    *,
    overrides: Mapping[str, Any] | None = None,
    env_prefix: str = DEFAULT_ENV_PREFIX,
) -> AssayConfig:
    """Load the assay configuration applying layered overrides."""

    base_data: dict[str, Any] = {}
    if config_path is not None:
        path = Path(config_path)
        if not path.exists():
            raise ConfigLoadError(f"Configuration file not found: {path}")
        try:
            with path.open("r", encoding="utf-8") as handle:
                base_data = yaml.safe_load(handle) or {}
        except yaml.YAMLError as exc:  # pragma: no cover - YAML errors are rare but handled
            raise ConfigLoadError(f"Failed to parse configuration file: {exc}") from exc
        except OSError as exc:  # pragma: no cover - filesystem errors
            raise ConfigLoadError(f"Unable to read configuration file: {exc}") from exc

    env_overrides = _load_env_overrides(env_prefix)
    merged = _merge_dicts(base_data, env_overrides)
    if overrides:
        merged = _merge_dicts(merged, overrides)

    try:
        return AssayConfig.model_validate(merged)
    except ValidationError as exc:  # pragma: no cover - delegated to caller tests
        raise ConfigLoadError(str(exc)) from exc


__all__ = [
    "ALLOWED_SOURCES",
    "ConfigLoadError",
    "DEFAULT_ENV_PREFIX",
    "DATE_TAG_FORMAT",
    "AssayConfig",
    "AssayHTTPGlobalSettings",
    "AssayHTTPRetrySettings",
    "AssayHTTPSettings",
    "AssayIOSettings",
    "AssayInputSettings",
    "AssayOutputSettings",
    "AssayRuntimeSettings",
    "AssayPostprocessSettings",
    "AssaySourceSettings",
    "AssaySourceHTTPSettings",
    "AssaySourcePaginationSettings",
    "SourceToggle",
    "CacheSettings",
    "FilterProfileSettings",
    "load_assay_config",
]
