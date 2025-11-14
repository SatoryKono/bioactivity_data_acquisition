"""Environment-driven configuration helpers for BioETL.

Responsibilities:

- чтение `.env`/process env через `EnvironmentSettings`;
- разрешение environment-слоёв (`configs/env/<name>`) без побочных эффектов;
- построение детерминированных override-мэппингов для коротких переменных;
- опциональная синхронизация `BIOETL__...` переменных для обратной совместимости.
"""

from __future__ import annotations

import os
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Iterable

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_VALID_ENVIRONMENTS: frozenset[str] = frozenset({"dev", "stage", "prod"})
_ENV_LAYER_PATTERNS: tuple[str, ...] = ("*.yaml", "*.yml")
ENV_ROOT_DIR = Path("configs/env")


class _EnvOverrideSpec:
    """Internal spec describing mapping between short env vars and config paths."""

    __slots__ = ("attr", "prefixed_key", "config_path")

    def __init__(self, attr: str, prefixed_key: str, config_path: Iterable[str]) -> None:
        self.attr = attr
        self.prefixed_key = prefixed_key
        self.config_path = tuple(config_path)


_ENV_OVERRIDE_SPECS: tuple[_EnvOverrideSpec, ...] = (
    _EnvOverrideSpec(
        "pubmed_tool",
        "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__TOOL",
        ("sources", "pubmed", "http", "identify", "tool"),
    ),
    _EnvOverrideSpec(
        "pubmed_email",
        "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__EMAIL",
        ("sources", "pubmed", "http", "identify", "email"),
    ),
    _EnvOverrideSpec(
        "pubmed_api_key",
        "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__API_KEY",
        ("sources", "pubmed", "http", "identify", "api_key"),
    ),
    _EnvOverrideSpec(
        "crossref_mailto",
        "BIOETL__SOURCES__CROSSREF__IDENTIFY__MAILTO",
        ("sources", "crossref", "identify", "mailto"),
    ),
    _EnvOverrideSpec(
        "semantic_scholar_api_key",
        "BIOETL__SOURCES__SEMANTIC_SCHOLAR__HTTP__HEADERS__X-API-KEY",
        ("sources", "semantic_scholar", "http", "headers", "x-api-key"),
    ),
    _EnvOverrideSpec(
        "iuphar_api_key",
        "BIOETL__SOURCES__IUPHAR__HTTP__HEADERS__X-API-KEY",
        ("sources", "iuphar", "http", "headers", "x-api-key"),
    ),
)


class EnvironmentSettings(BaseSettings):
    """Typed view of BioETL environment variables."""

    model_config = SettingsConfigDict(
        env_file=(".env",),
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    bioetl_env: str | None = Field(default=None, alias="BIOETL_ENV")
    pubmed_tool: str | None = Field(default=None, alias="PUBMED_TOOL")
    pubmed_email: str | None = Field(default=None, alias="PUBMED_EMAIL")
    pubmed_api_key: SecretStr | None = Field(default=None, alias="PUBMED_API_KEY")
    crossref_mailto: str | None = Field(default=None, alias="CROSSREF_MAILTO")
    semantic_scholar_api_key: SecretStr | None = Field(
        default=None, alias="SEMANTIC_SCHOLAR_API_KEY"
    )
    iuphar_api_key: SecretStr | None = Field(default=None, alias="IUPHAR_API_KEY")
    vocab_store: Path | None = Field(default=None, alias="VOCAB_STORE")
    offline_chembl_client: bool = Field(default=False, alias="BIOETL_OFFLINE_CHEMBL_CLIENT")

    @field_validator("bioetl_env")
    @classmethod
    def _validate_environment(cls, value: str | None) -> str | None:
        """Normalise and validate the selected BioETL environment."""
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            return None
        if normalized not in _VALID_ENVIRONMENTS:
            allowed = ", ".join(sorted(_VALID_ENVIRONMENTS))
            msg = f"BIOETL_ENV must be one of: {allowed}"
            raise ValueError(msg)
        return normalized

    @field_validator("vocab_store")
    @classmethod
    def _resolve_vocab_store(cls, value: Path | None) -> Path | None:
        """Expand and resolve the configured vocabulary store path."""
        if value is None:
            return None
        return value.expanduser().resolve()

    @field_validator("offline_chembl_client", mode="before")
    @classmethod
    def _coerce_bool(cls, value: Any) -> bool:
        """Coerce environment values into booleans."""
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off"}:
                return False
        return bool(value)

    @field_validator("pubmed_tool")
    @classmethod
    def _normalize_pubmed_tool(cls, value: str | None) -> str | None:
        """Trim the PubMed tool identifier and treat empty strings as None."""
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @field_validator("pubmed_email", "crossref_mailto")
    @classmethod
    def _validate_contact_email(cls, value: str | None) -> str | None:
        """Validate contact email format while allowing empty values."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            return None
        if "@" not in normalized:
            msg = "email address must contain '@'"
            raise ValueError(msg)
        return normalized


def load_environment_settings(*, env_file: Path | None = None) -> EnvironmentSettings:
    """Load and validate BioETL environment settings.

    Parameters
    ----------
    env_file:
        Optional path to a ``.env`` file. When omitted, the default search order
        from :class:`EnvironmentSettings` is used.
    """

    init_kwargs: dict[str, Any] = {}
    if env_file is not None:
        init_kwargs["_env_file"] = env_file
    return EnvironmentSettings(**init_kwargs)


def build_env_override_mapping(settings: EnvironmentSettings) -> dict[str, Any]:
    """Return nested overrides derived from short environment variables."""

    overrides: dict[str, Any] = {}

    for spec in _ENV_OVERRIDE_SPECS:
        value = _extract_plain_value(getattr(settings, spec.attr))
        if value is None:
            continue
        _assign_nested_override(overrides, spec.config_path, value)

    return overrides


def resolve_env_layers(
    environment: str | None,
    *,
    base: Path,
    env_root: Path | None = None,
) -> list[Path]:
    """Discover environment-specific YAML layers for the selected environment."""

    if environment is None:
        return []

    root_dir = env_root if env_root is not None else ENV_ROOT_DIR
    target_dir = root_dir / environment
    resolved_dir = _resolve_directory(target_dir, base=base)

    if not resolved_dir.exists():
        msg = f"Configuration directory not found for environment '{environment}': {resolved_dir}"
        raise FileNotFoundError(msg)

    files: set[Path] = set()
    for pattern in _ENV_LAYER_PATTERNS:
        for candidate in resolved_dir.glob(pattern):
            if candidate.is_file():
                files.add(candidate.resolve())

    return sorted(files)


def apply_runtime_overrides(
    settings: EnvironmentSettings,
    *,
    environ: MutableMapping[str, str] | None = None,
) -> dict[str, str]:
    """Populate ``BIOETL__`` overrides from short environment variables."""

    target = environ if environ is not None else os.environ
    applied: dict[str, str] = {}

    for key, value in _build_prefixed_runtime_variables(settings).items():
        if key in target:
            continue
        target[key] = value
        applied[key] = value

    return applied


def _extract_plain_value(value: str | SecretStr | None) -> str | None:
    """Normalize env values (including SecretStr) into plain strings."""
    if value is None:
        return None
    if isinstance(value, SecretStr):
        plain = value.get_secret_value()
    else:
        plain = value
    plain = plain.strip()
    return plain or None


def _assign_nested_override(
    target: MutableMapping[str, Any],
    path: Iterable[str],
    value: str,
) -> None:
    """Assign a value to a nested dictionary without mutating siblings."""
    current: MutableMapping[str, Any] = target
    parts = tuple(path)
    for part in parts[:-1]:
        existing = current.get(part)
        if not isinstance(existing, MutableMapping):
            next_level: dict[str, Any] = {}
            current[part] = next_level
            current = next_level
            continue
        current = existing
    current[parts[-1]] = value


def _build_prefixed_runtime_variables(settings: EnvironmentSettings) -> dict[str, str]:
    """Return mapping of ``BIOETL__...`` keys derived from short variables."""
    overrides: dict[str, str] = {}
    for spec in _ENV_OVERRIDE_SPECS:
        value = _extract_plain_value(getattr(settings, spec.attr))
        if value is None:
            continue
        overrides.setdefault(spec.prefixed_key, value)
    return overrides


def _resolve_directory(directory: Path, *, base: Path) -> Path:
    """Resolve ``directory`` relative to ``base``/parents/current working dir."""
    if directory.is_absolute():
        return directory.resolve()

    search_roots: list[Path] = [base, *base.parents, Path.cwd()]
    for root in search_roots:
        candidate = (root / directory).resolve()
        if candidate.exists():
            return candidate

    return (Path.cwd() / directory).resolve()


__all__ = [
    "EnvironmentSettings",
    "ENV_ROOT_DIR",
    "apply_runtime_overrides",
    "build_env_override_mapping",
    "load_environment_settings",
    "resolve_env_layers",
]

