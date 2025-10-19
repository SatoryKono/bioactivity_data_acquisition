"""Configuration models and loader for the activity ETL pipeline."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from library.config import (
    DeterminismSettings,
    LoggingSettings,
    _assign_path,
    _merge_dicts,
    _parse_scalar,
)

ALLOWED_SOURCES: tuple[str, ...] = ("chembl",)
DATE_TAG_FORMAT = "%Y%m%d"
DATE_TAG_REGEX = r"^\d{8}$"
DEFAULT_ENV_PREFIX = "BIOACTIVITY__"


class ActivityInputSettings(BaseModel):
    """Input configuration for the activity pipeline."""

    activity_csv: Path = Field(default=Path("data/input/activity.csv"))


class ActivityOutputSettings(BaseModel):
    """Output configuration for generated artefacts."""

    dir: Path = Field(default=Path("data/output/activity"))


class ActivityIOSettings(BaseModel):
    """Container for I/O configuration."""

    input: ActivityInputSettings = Field(default_factory=ActivityInputSettings)
    output: ActivityOutputSettings = Field(default_factory=ActivityOutputSettings)


class ActivityRuntimeSettings(BaseModel):
    """Runtime toggles controlled via CLI or environment variables."""

    date_tag: str | None = Field(default=None, pattern=DATE_TAG_REGEX)
    workers: int = Field(default=4, ge=1, le=64)
    limit: int | None = Field(default=None, ge=1)
    dry_run: bool = Field(default=False)


class SourceToggle(BaseModel):
    """Enable or disable a specific source."""

    enabled: bool = Field(default=True)


class ActivityHTTPRetrySettings(BaseModel):
    """Retry configuration for HTTP calls."""

    total: int = Field(default=5, ge=0, le=10)
    backoff_multiplier: float = Field(default=2.0, gt=0.0, le=10.0)


class ActivityHTTPGlobalSettings(BaseModel):
    """Global HTTP behaviour applied to all sources."""

    timeout_sec: float = Field(default=30.0, gt=0.0, le=600.0)
    retries: ActivityHTTPRetrySettings = Field(default_factory=ActivityHTTPRetrySettings)
    headers: dict[str, str] = Field(default_factory=dict)


class ActivityHTTPSettings(BaseModel):
    """HTTP configuration namespace."""

    model_config = ConfigDict(populate_by_name=True)

    global_: ActivityHTTPGlobalSettings = Field(default_factory=ActivityHTTPGlobalSettings, alias="global")


class ActivitySourceHTTPSettings(BaseModel):
    """HTTP settings specific to an activity source."""

    base_url: str | None = Field(default=None)
    timeout_sec: float | None = Field(default=None, gt=0.0, le=600.0)
    headers: dict[str, str] = Field(default_factory=dict)
    retries: dict[str, Any] = Field(default_factory=dict)


class ActivitySourcePaginationSettings(BaseModel):
    """Pagination settings for an activity source."""

    page_param: str | None = Field(default=None)
    size_param: str | None = Field(default=None)
    size: int | None = Field(default=None, ge=1, le=1000)
    max_pages: int | None = Field(default=None, ge=1, le=1000)


class BatchSettings(BaseModel):
    """Настройки batch-запросов."""

    enabled: bool = Field(default=False, description="Включить batch режим")
    size: int = Field(default=50, ge=1, le=1000, description="Размер batch (кол-во ID в запросе)")
    max_batches: int | None = Field(default=None, ge=1, description="Максимальное число batches")
    parallel_workers: int = Field(default=4, ge=1, le=32, description="Кол-во параллельных воркеров")
    id_field: str = Field(default="activity_chembl_id", description="Имя поля с ID во входном CSV")
    filter_param: str = Field(default="activity_id__in", description="Параметр API для фильтрации по ID")


class ActivitySourceSettings(BaseModel):
    """Configuration settings for an activity source."""

    enabled: bool = Field(default=True)
    name: str | None = Field(default=None)
    endpoint: str | None = Field(default=None)
    params: dict[str, Any] = Field(default_factory=dict)
    pagination: ActivitySourcePaginationSettings = Field(default_factory=ActivitySourcePaginationSettings)
    http: ActivitySourceHTTPSettings = Field(default_factory=ActivitySourceHTTPSettings)
    rate_limit: dict[str, Any] = Field(default_factory=dict)
    batch: BatchSettings = Field(default_factory=BatchSettings)


def _default_sources() -> dict[str, ActivitySourceSettings]:
    return {name: ActivitySourceSettings(name=name) for name in ALLOWED_SOURCES}


class ActivityPostprocessSettings(BaseModel):
    """Postprocessing configuration for activity (QC on by default)."""

    qc: SourceToggle = Field(default_factory=lambda: SourceToggle(enabled=True))
    correlation: SourceToggle = Field(default_factory=lambda: SourceToggle(enabled=False))


class ActivityConfig(BaseModel):
    """Top-level configuration for the activity ETL pipeline."""

    io: ActivityIOSettings = Field(default_factory=ActivityIOSettings)
    runtime: ActivityRuntimeSettings = Field(default_factory=ActivityRuntimeSettings)
    http: ActivityHTTPSettings = Field(default_factory=ActivityHTTPSettings)
    sources: dict[str, ActivitySourceSettings] = Field(default_factory=_default_sources)
    postprocess: ActivityPostprocessSettings = Field(default_factory=ActivityPostprocessSettings)
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
    def _validate_sources(self) -> ActivityConfig:
        if not any(toggle.enabled for toggle in self.sources.values()):
            raise ValueError("At least one source must be enabled")
        return self

    def enabled_sources(self) -> list[str]:
        """Return the list of enabled sources respecting declaration order."""

        return [name for name in ALLOWED_SOURCES if self.sources.get(name, ActivitySourceSettings()).enabled]


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


def load_activity_config(
    config_path: Path | str | None,
    *,
    overrides: Mapping[str, Any] | None = None,
    env_prefix: str = DEFAULT_ENV_PREFIX,
) -> ActivityConfig:
    """Load the activity configuration applying layered overrides."""

    base_data: dict[str, Any] = {}
    if config_path is not None:
        path = Path(config_path)
        if not path.exists():
            raise ConfigLoadError(f"Configuration file not found: {path}")
        try:
            with path.open("r", encoding="utf-8") as handle:
                base_data = yaml.safe_load(handle) or {}
        except yaml.YAMLError as exc:  # pragma: no cover
            raise ConfigLoadError(f"Failed to parse configuration file: {exc}") from exc
        except OSError as exc:  # pragma: no cover
            raise ConfigLoadError(f"Unable to read configuration file: {exc}") from exc

    env_overrides = _load_env_overrides(env_prefix)
    merged = _merge_dicts(base_data, env_overrides)
    if overrides:
        merged = _merge_dicts(merged, overrides)

    try:
        return ActivityConfig.model_validate(merged)
    except ValidationError as exc:  # pragma: no cover
        raise ConfigLoadError(str(exc)) from exc


__all__ = [
    "ALLOWED_SOURCES",
    "ConfigLoadError",
    "DEFAULT_ENV_PREFIX",
    "DATE_TAG_FORMAT",
    "ActivityConfig",
    "ActivityHTTPGlobalSettings",
    "ActivityHTTPRetrySettings",
    "ActivityHTTPSettings",
    "ActivityIOSettings",
    "ActivityInputSettings",
    "ActivityOutputSettings",
    "ActivityRuntimeSettings",
    "ActivityPostprocessSettings",
    "ActivitySourceSettings",
    "ActivitySourceHTTPSettings",
    "ActivitySourcePaginationSettings",
    "SourceToggle",
    "load_activity_config",
]
