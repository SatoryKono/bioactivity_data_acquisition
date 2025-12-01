from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .environment_utils import (
    coerce_bool,
    normalize_env_name,
    normalize_tool,
    resolve_vocab_store,
    validate_email,
)


class EnvironmentSettings(BaseSettings):
    """Typed view of BioETL environment variables."""

    model_config = SettingsConfigDict(
        env_file=(".env",), env_file_encoding="utf-8", extra="ignore", populate_by_name=True
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
    def _normalize_env(cls, value: str | None) -> str | None:
        return normalize_env_name(value)

    @field_validator("vocab_store")
    @classmethod
    def _resolve_vocab(cls, value: Path | None) -> Path | None:
        return resolve_vocab_store(value)

    @field_validator("offline_chembl_client", mode="before")
    @classmethod
    def _coerce_bool(cls, value: Any) -> bool:
        return coerce_bool(value)

    @field_validator("pubmed_tool")
    @classmethod
    def _trim_tool(cls, value: str | None) -> str | None:
        return normalize_tool(value)

    @field_validator("pubmed_email", "crossref_mailto")
    @classmethod
    def _validate_email(cls, value: str | None) -> str | None:
        return validate_email(value)


def load_environment_settings(*, env_file: Path | None = None) -> EnvironmentSettings:
    """Load environment settings from ``env_file`` or process environment."""

    kwargs: dict[str, Any] = {}
    if env_file is not None:
        kwargs["_env_file"] = env_file
    return EnvironmentSettings(**kwargs)


__all__ = ["EnvironmentSettings", "load_environment_settings"]
