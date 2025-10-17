"""Configuration models and loader for the document ETL pipeline."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from library.config import _assign_path, _merge_dicts, _parse_scalar, DeterminismSettings, LoggingSettings

ALLOWED_SOURCES: tuple[str, ...] = ("chembl", "crossref", "openalex", "pubmed", "semantic_scholar")
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

    date_tag: str | None = Field(
        default=None, pattern=DATE_TAG_REGEX
    )
    workers: int = Field(default=4, ge=1, le=64)
    limit: int | None = Field(default=None, ge=1)
    dry_run: bool = Field(default=False)


class CitationFormattingSettings(BaseModel):
    """Configuration for citation formatting."""
    
    enabled: bool = Field(default=True)
    columns: dict[str, str] = Field(default_factory=lambda: {
        "journal": "journal",
        "volume": "volume", 
        "issue": "issue",
        "first_page": "first_page",
        "last_page": "last_page"
    })


class DocumentPostprocessSettings(BaseModel):
    """Postprocessing configuration for documents."""

    qc: SourceToggle = Field(default_factory=lambda: SourceToggle(enabled=True))
    correlation: SourceToggle = Field(default_factory=lambda: SourceToggle(enabled=False))
    journal_normalization: SourceToggle = Field(default_factory=lambda: SourceToggle(enabled=True))
    citation_formatting: CitationFormattingSettings = Field(default_factory=CitationFormattingSettings)


class DocumentHTTPRetrySettings(BaseModel):
    """Retry configuration for HTTP calls."""

    total: int = Field(default=5, ge=0, le=10)
    backoff_multiplier: float = Field(default=2.0, gt=0.0, le=10.0)


class DocumentHTTPGlobalSettings(BaseModel):
    """Global HTTP behaviour applied to all sources."""

    timeout_sec: float = Field(default=30.0, gt=0.0, le=600.0)
    retries: DocumentHTTPRetrySettings = Field(default_factory=DocumentHTTPRetrySettings)
    headers: dict[str, str] = Field(default_factory=dict)


class DocumentHTTPSettings(BaseModel):
    """HTTP configuration namespace."""

    model_config = ConfigDict(populate_by_name=True)

    global_: DocumentHTTPGlobalSettings = Field(
        default_factory=DocumentHTTPGlobalSettings, alias="global"
    )


class SourceToggle(BaseModel):
    """Enable or disable a specific source."""

    enabled: bool = Field(default=True)


class DocumentSourceHTTPSettings(BaseModel):
    """HTTP settings specific to a document source."""
    
    base_url: str | None = Field(default=None)
    timeout_sec: float | None = Field(default=None, gt=0.0, le=600.0)
    headers: dict[str, str] = Field(default_factory=dict)
    retries: dict[str, Any] = Field(default_factory=dict)


class DocumentSourcePaginationSettings(BaseModel):
    """Pagination settings for a document source."""
    
    page_param: str | None = Field(default=None)
    size_param: str | None = Field(default=None)
    size: int | None = Field(default=None, ge=1, le=1000)
    max_pages: int | None = Field(default=None, ge=1, le=1000)


class DocumentSourceSettings(BaseModel):
    """Configuration settings for a document source."""
    
    enabled: bool = Field(default=True)
    name: str | None = Field(default=None)
    endpoint: str | None = Field(default=None)
    params: dict[str, Any] = Field(default_factory=dict)
    pagination: DocumentSourcePaginationSettings = Field(default_factory=DocumentSourcePaginationSettings)
    http: DocumentSourceHTTPSettings = Field(default_factory=DocumentSourceHTTPSettings)
    rate_limit: dict[str, Any] = Field(default_factory=dict)


def _default_sources() -> dict[str, DocumentSourceSettings]:
    return {name: DocumentSourceSettings(name=name) for name in ALLOWED_SOURCES}


class DocumentConfig(BaseModel):
    """Top-level configuration for the document ETL pipeline."""

    io: DocumentIOSettings = Field(default_factory=DocumentIOSettings)
    runtime: DocumentRuntimeSettings = Field(default_factory=DocumentRuntimeSettings)
    http: DocumentHTTPSettings = Field(default_factory=DocumentHTTPSettings)
    sources: dict[str, DocumentSourceSettings] = Field(default_factory=_default_sources)
    postprocess: DocumentPostprocessSettings = Field(default_factory=DocumentPostprocessSettings)
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
    def _validate_sources(self) -> DocumentConfig:
        if not any(toggle.enabled for toggle in self.sources.values()):
            raise ValueError("At least one source must be enabled")
        return self

    def enabled_sources(self) -> list[str]:
        """Return the list of enabled sources respecting declaration order."""

        return [name for name in ALLOWED_SOURCES if self.sources.get(name, DocumentSourceSettings()).enabled]


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
    "DocumentPostprocessSettings",
    "DocumentSourceSettings",
    "DocumentSourceHTTPSettings",
    "DocumentSourcePaginationSettings",
    "SourceToggle",
    "load_document_config",
]
