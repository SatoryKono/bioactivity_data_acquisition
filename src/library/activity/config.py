"""Pydantic-based configuration for activity ETL (aligned with assays)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from collections.abc import Mapping

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from library.config import (
    APIClientConfig,
    DeterminismSettings,
    LoggingSettings,
    RateLimitSettings,
    RetrySettings,
    _assign_path,
    _merge_dicts,
    _parse_scalar,
)


ALLOWED_SOURCES: tuple[str, ...] = ("chembl",)
DEFAULT_ENV_PREFIX = "BIOACTIVITY__"


class ActivityCSVSettings(BaseModel):
    encoding: str = Field(default="utf-8")
    float_format: str | None = Field(default=None)
    date_format: str | None = Field(default=None)
    na_rep: str | None = Field(default=None)
    line_terminator: str | None = Field(default=None)


class ActivityParquetSettings(BaseModel):
    compression: str = Field(default="snappy")


class ActivityOutputSettings(BaseModel):
    dir: Path = Field(default=Path("data/output/activity"))
    format: str = Field(default="csv")
    csv: ActivityCSVSettings = Field(default_factory=ActivityCSVSettings)
    parquet: ActivityParquetSettings = Field(default_factory=ActivityParquetSettings)


class ActivityIOSettings(BaseModel):
    output: ActivityOutputSettings = Field(default_factory=ActivityOutputSettings)
    cache_dir: Path = Field(default=Path("data/cache/activity/raw"))


class ActivityRuntimeSettings(BaseModel):
    date_tag: str | None = Field(default=None, pattern=r"^\d{8}$")
    workers: int = Field(default=4, ge=1, le=64)
    limit: int | None = Field(default=None, ge=1)
    dry_run: bool = Field(default=False)


class ActivityHTTPRetrySettings(BaseModel):
    total: int = Field(default=5, ge=0, le=10)
    backoff_multiplier: float = Field(default=2.0, gt=0.0, le=10.0)


class ActivityHTTPGlobalSettings(BaseModel):
    timeout_sec: float = Field(default=60.0, gt=0.0, le=600.0)
    retries: ActivityHTTPRetrySettings = Field(default_factory=ActivityHTTPRetrySettings)
    headers: dict[str, str] = Field(default_factory=dict)
    rate_limit: dict[str, Any] = Field(default_factory=dict)


class ActivityHTTPSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    global_: ActivityHTTPGlobalSettings = Field(default_factory=ActivityHTTPGlobalSettings, alias="global")


class ActivitySourceHTTPSettings(BaseModel):
    base_url: str | None = Field(default=None)
    timeout_sec: float | None = Field(default=None, gt=0.0, le=600.0)
    headers: dict[str, str] = Field(default_factory=dict)
    retries: dict[str, Any] = Field(default_factory=dict)


class ActivitySourcePaginationSettings(BaseModel):
    page_param: str | None = Field(default=None)
    size_param: str | None = Field(default=None)
    offset_param: str | None = Field(default=None)
    size: int | None = Field(default=None, ge=1, le=1000)
    max_pages: int | None = Field(default=None, ge=1, le=1000)


class ActivitySourceSettings(BaseModel):
    enabled: bool = Field(default=True)
    name: str | None = Field(default=None)
    endpoint: str | None = Field(default=None)
    params: dict[str, Any] = Field(default_factory=dict)
    pagination: ActivitySourcePaginationSettings = Field(default_factory=ActivitySourcePaginationSettings)
    http: ActivitySourceHTTPSettings = Field(default_factory=ActivitySourceHTTPSettings)


class ActivityQCSettings(BaseModel):
    enabled: bool = Field(default=True)
    enhanced: bool = Field(default=False)


class ActivityCorrelationSettings(BaseModel):
    enabled: bool = Field(default=True)
    enhanced: bool = Field(default=False)


class ActivityPostprocessSettings(BaseModel):
    qc: ActivityQCSettings = Field(default_factory=ActivityQCSettings)
    correlation: ActivityCorrelationSettings = Field(default_factory=ActivityCorrelationSettings)


class CacheSettings(BaseModel):
    enabled: bool = Field(default=True)
    directory: Path = Field(default=Path(".cache/chembl"))
    ttl: int = Field(default=3600, ge=1, le=86400)
    invalidate_on_release_change: bool = Field(default=True)


def _default_sources() -> dict[str, ActivitySourceSettings]:
    return {name: ActivitySourceSettings(name=name) for name in ALLOWED_SOURCES}


class ActivityConfig(BaseModel):
    """Top-level configuration for the activity ETL pipeline (assay-aligned)."""

    io: ActivityIOSettings = Field(default_factory=ActivityIOSettings)
    runtime: ActivityRuntimeSettings = Field(default_factory=ActivityRuntimeSettings)
    http: ActivityHTTPSettings = Field(default_factory=ActivityHTTPSettings)
    sources: dict[str, ActivitySourceSettings] = Field(default_factory=_default_sources)
    postprocess: ActivityPostprocessSettings = Field(default_factory=ActivityPostprocessSettings)
    determinism: DeterminismSettings = Field(default_factory=DeterminismSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)

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

    # Backward-compatible helpers used by current pipeline/CLI
    @classmethod
    def from_yaml(cls, path: Path | str) -> ActivityConfig:  # type: ignore[name-defined]
        return load_activity_config(path)

    def get_output_path(self) -> Path:
        return self.io.output.dir

    def get_cache_path(self) -> Path:
        return self.io.cache_dir

    def ensure_directories(self) -> None:
        self.get_output_path().mkdir(parents=True, exist_ok=True)
        self.get_cache_path().mkdir(parents=True, exist_ok=True)

    @property
    def limit(self) -> int | None:
        return self.runtime.limit

    @property
    def dry_run(self) -> bool:
        return self.runtime.dry_run

    @dry_run.setter
    def dry_run(self, value: bool) -> None:  # pragma: no cover - CLI override support
        self.runtime.dry_run = value

    @property
    def timeout_sec(self) -> float:
        return self.http.global_.timeout_sec

    @timeout_sec.setter
    def timeout_sec(self, value: float) -> None:  # pragma: no cover - CLI override support
        self.http.global_.timeout_sec = value

    @property
    def max_retries(self) -> int:
        return self.http.global_.retries.total

    @max_retries.setter
    def max_retries(self, value: int) -> None:  # pragma: no cover - CLI override support
        self.http.global_.retries.total = value

    def to_api_client_config(self) -> APIClientConfig:
        # Build headers (global only for now)
        default_headers = {
            "User-Agent": "bioactivity-data-acquisition/0.1.0",
            "Accept": "application/json",
        }
        headers = {**default_headers, **self.http.global_.headers}

        # Resolve secrets placeholders
        processed_headers: dict[str, str] = {}
        for key, value in headers.items():
            if isinstance(value, str):
                import re

                def replace_placeholder(match: Any) -> str:
                    secret_name = match.group(1)
                    env_var = os.environ.get(secret_name.upper())
                    return env_var if env_var is not None else match.group(0)

                processed_value = re.sub(r"\{([^}]+)\}", replace_placeholder, value)
                if processed_value and processed_value.strip() and not (
                    processed_value.startswith("{") and processed_value.endswith("}")
                ):
                    processed_headers[key] = processed_value
            else:
                processed_headers[key] = value  # pragma: no cover - non-str headers are rare

        # Resolve base_url
        src = self.sources.get("chembl") or ActivitySourceSettings()
        base_url = src.http.base_url or "https://www.ebi.ac.uk/chembl/api/data"

        # Resolve retries
        retry_settings = RetrySettings(
            total=src.http.retries.get("total", self.http.global_.retries.total),
            backoff_multiplier=src.http.retries.get(
                "backoff_multiplier", self.http.global_.retries.backoff_multiplier
            ),
        )

        # Optional rate limit
        rate_limit = None
        if self.http.global_.rate_limit:
            max_calls = self.http.global_.rate_limit.get("max_calls")
            period = self.http.global_.rate_limit.get("period")
            if max_calls is not None and period is not None:
                rate_limit = RateLimitSettings(max_calls=max_calls, period=period)

        return APIClientConfig(
            name="chembl_activity",
            base_url=base_url,  # type: ignore[arg-type]
            headers=processed_headers,
            timeout=src.http.timeout_sec or self.http.global_.timeout_sec,
            retries=retry_settings,
            rate_limit=rate_limit,
        )


class ConfigLoadError(RuntimeError):
    pass


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

    # Legacy compatibility mapping
    # Map io.cache.raw_dir -> io.cache_dir
    try:
        legacy_cache = (
            merged.get("io", {})
            .get("cache", {})
            .get("raw_dir")
        )
        if legacy_cache and isinstance(legacy_cache, (str, Path)):
            merged.setdefault("io", {})["cache_dir"] = legacy_cache
    except Exception:
        pass

    try:
        return ActivityConfig.model_validate(merged)
    except ValidationError as exc:
        raise ConfigLoadError(str(exc)) from exc


__all__ = [
    "ActivityConfig",
    "load_activity_config",
    "ConfigLoadError",
]
