"""Environment-driven configuration helpers for BioETL.

Этот модуль концентрирует чтение переменных окружения и обеспечивает
сопоставление «коротких» переменных (например, ``PUBMED_TOOL``) с вложенными
ключами ``BIOETL__...``. Такой подход рождает единый источник правды и упрощает
использование 12-Factor Config.
"""

from __future__ import annotations

import os
import warnings
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .utils import coerce_bool


_VALID_ENVIRONMENTS: frozenset[str] = frozenset({"dev", "stage", "prod"})


class EnvironmentSettings(BaseSettings):
    """Typed view of BioETL environment variables."""

    model_config = SettingsConfigDict(
        env_file=(".env",),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    bioetl_env: str = Field(default="dev", alias="BIOETL_ENV")
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
    def _validate_environment(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized:
            return "dev"
        if normalized not in _VALID_ENVIRONMENTS:
            allowed = ", ".join(sorted(_VALID_ENVIRONMENTS))
            msg = f"BIOETL_ENV must be one of: {allowed}"
            raise ValueError(msg)
        return normalized

    @field_validator("vocab_store")
    @classmethod
    def _resolve_vocab_store(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        return value.expanduser().resolve()

    @field_validator("offline_chembl_client", mode="before")
    @classmethod
    def _coerce_bool(cls, value: Any) -> bool:
        return coerce_bool(value)

    @field_validator("pubmed_tool")
    @classmethod
    def _normalize_pubmed_tool(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @field_validator("pubmed_email", "crossref_mailto")
    @classmethod
    def _validate_contact_email(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            return None
        if "@" not in normalized:
            msg = "email address must contain '@'"
            raise ValueError(msg)
        return normalized


def read_environment_settings(*, env_file: Path | None = None) -> EnvironmentSettings:
    """Read and validate BioETL environment settings.

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


def load_environment_settings(*, env_file: Path | None = None) -> EnvironmentSettings:
    """Deprecated wrapper for :func:`read_environment_settings`."""

    warnings.warn(
        "load_environment_settings() is deprecated and will be removed in a future "
        "release; use read_environment_settings() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return read_environment_settings(env_file=env_file)


def apply_runtime_overrides(
    settings: EnvironmentSettings,
    *,
    environ: MutableMapping[str, str] | None = None,
) -> dict[str, str]:
    """Populate ``BIOETL__`` overrides from short environment variables.

    Parameters
    ----------
    settings:
        Previously loaded :class:`EnvironmentSettings`.
    environ:
        Target mapping to mutate. Defaults to :data:`os.environ`.

    Returns
    -------
    dict[str, str]
        Mapping of the keys that were newly injected into ``environ``.
    """

    target = environ if environ is not None else os.environ
    applied: dict[str, str] = {}

    def _set_if_missing(key: str, value: str | SecretStr | None) -> None:
        if value is None:
            return
        if isinstance(value, SecretStr):
            secret_value = value.get_secret_value()
            if not secret_value:
                return
            value_str = secret_value
        else:
            value_str = value
        if not value_str:
            return
        if key in target:
            return
        target[key] = value_str
        applied[key] = value_str

    _set_if_missing(
        "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__TOOL",
        settings.pubmed_tool,
    )
    _set_if_missing(
        "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__EMAIL",
        settings.pubmed_email,
    )
    _set_if_missing(
        "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__API_KEY",
        settings.pubmed_api_key,
    )
    _set_if_missing(
        "BIOETL__SOURCES__CROSSREF__IDENTIFY__MAILTO",
        settings.crossref_mailto,
    )
    _set_if_missing(
        "BIOETL__SOURCES__SEMANTIC_SCHOLAR__HTTP__HEADERS__X-API-KEY",
        settings.semantic_scholar_api_key,
    )
    _set_if_missing(
        "BIOETL__SOURCES__IUPHAR__HTTP__HEADERS__X-API-KEY",
        settings.iuphar_api_key,
    )

    return applied


__all__ = [
    "EnvironmentSettings",
    "apply_runtime_overrides",
    "read_environment_settings",
    "load_environment_settings",
]

