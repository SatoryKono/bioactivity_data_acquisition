"""Configuration models and loader for the document ETL pipeline."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from library.config import _assign_path, _merge_dicts, _parse_scalar

ALLOWED_SOURCES: tuple[str, ...] = ("chembl", "crossref", "openalex")
DATE_TAG_FORMAT = "%Y%m%d"
DATE_TAG_REGEX = r"^\d{8}$"
DEFAULT_ENV_PREFIX = "BIOACTIVITY__"


class DocumentInputSettings(BaseModel):
    """Input configuration for the document pipeline."""

    documents_csv: Path = Field(default=Path("data/input/documents.csv"))


class DocumentOutputSettings(BaseModel):
    """Output configuration for generated artefacts."""

    dir: Path = Field(default=Path("data/output/documents"))


class DocumentIOSettings(BaseModel):
    """Container for I/O configuration."""

    input: DocumentInputSettings = Field(default_factory=DocumentInputSettings)
    output: DocumentOutputSettings = Field(default_factory=DocumentOutputSettings)


class DocumentRuntimeSettings(BaseModel):
    """Runtime toggles controlled via CLI or environment variables."""

    date_tag: str = Field(
        default_factory=lambda: datetime.utcnow().strftime(DATE_TAG_FORMAT), pattern=DATE_TAG_REGEX
    )
    workers: int = Field(default=4, ge=1, le=64)
    limit: int | None = Field(default=None, ge=1)
    dry_run: bool = Field(default=False)


class DocumentHTTPRetrySettings(BaseModel):
    """Retry configuration for HTTP calls."""

    total: int = Field(default=5, ge=0, le=10)


class DocumentHTTPGlobalSettings(BaseModel):
    """Global HTTP behaviour applied to all sources."""

    timeout_sec: float = Field(default=30.0, gt=0.0, le=600.0)
    retries: DocumentHTTPRetrySettings = Field(default_factory=DocumentHTTPRetrySettings)


class DocumentHTTPSettings(BaseModel):
    """HTTP configuration namespace."""

    model_config = ConfigDict(populate_by_name=True)

    global_: DocumentHTTPGlobalSettings = Field(
        default_factory=DocumentHTTPGlobalSettings, alias="global"
    )


class SourceToggle(BaseModel):
    """Enable or disable a specific source."""

    enabled: bool = Field(default=True)


def _default_sources() -> dict[str, SourceToggle]:
    return {name: SourceToggle() for name in ALLOWED_SOURCES}


class DocumentConfig(BaseModel):
    """Top-level configuration for the document ETL pipeline."""

    io: DocumentIOSettings = Field(default_factory=DocumentIOSettings)
    runtime: DocumentRuntimeSettings = Field(default_factory=DocumentRuntimeSettings)
    http: DocumentHTTPSettings = Field(default_factory=DocumentHTTPSettings)
    sources: dict[str, SourceToggle] = Field(default_factory=_default_sources)

    model_config = ConfigDict(extra="ignore")

    @field_validator("sources", mode="before")
    @classmethod
    def _normalise_sources(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {name: {"enabled": True} for name in ALLOWED_SOURCES}
        if isinstance(value, Mapping):
            normalised: dict[str, Any] = {}
            for raw_key, raw_value in value.items():
                key = str(raw_key).lower()
                if key not in ALLOWED_SOURCES:
                    raise ValueError(f"Unsupported source '{raw_key}'")
                if isinstance(raw_value, Mapping):
                    normalised[key] = dict(raw_value)
                elif isinstance(raw_value, bool):
                    normalised[key] = {"enabled": raw_value}
                else:
                    raise ValueError(f"Invalid toggle for source '{raw_key}'")
            for name in ALLOWED_SOURCES:
                normalised.setdefault(name, {"enabled": True})
            return normalised
        raise ValueError("sources must be a mapping of toggles")

    @model_validator(mode="after")
    def _validate_sources(self) -> DocumentConfig:
        if not any(toggle.enabled for toggle in self.sources.values()):
            raise ValueError("At least one source must be enabled")
        return self

    def enabled_sources(self) -> list[str]:
        """Return the list of enabled sources respecting declaration order."""

        return [name for name in ALLOWED_SOURCES if self.sources.get(name, SourceToggle()).enabled]


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


def load_document_config(
    config_path: Path | str | None,
    *,
    overrides: Mapping[str, Any] | None = None,
    env_prefix: str = DEFAULT_ENV_PREFIX,
) -> DocumentConfig:
    """Load the document configuration applying layered overrides."""

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
        return DocumentConfig.model_validate(merged)
    except ValidationError as exc:  # pragma: no cover - delegated to caller tests
        raise ConfigLoadError(str(exc)) from exc


__all__ = [
    "ALLOWED_SOURCES",
    "ConfigLoadError",
    "DEFAULT_ENV_PREFIX",
    "DATE_TAG_FORMAT",
    "DocumentConfig",
    "DocumentHTTPGlobalSettings",
    "DocumentHTTPRetrySettings",
    "DocumentHTTPSettings",
    "DocumentIOSettings",
    "DocumentInputSettings",
    "DocumentOutputSettings",
    "DocumentRuntimeSettings",
    "SourceToggle",
    "load_document_config",
]
