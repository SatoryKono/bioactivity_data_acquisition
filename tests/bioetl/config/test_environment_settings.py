"""Tests for env settings helpers."""

from __future__ import annotations

import pytest

from pydantic import SecretStr

from bioetl.config import apply_runtime_overrides, load_environment_settings
from bioetl.config.environment import EnvironmentSettings


@pytest.mark.unit
def test_load_environment_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BIOETL_ENV", raising=False)
    monkeypatch.delenv("PUBMED_TOOL", raising=False)

    settings = load_environment_settings()

    assert settings.bioetl_env == "dev"
    assert settings.pubmed_tool is None


@pytest.mark.unit
def test_load_environment_settings_invalid_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIOETL_ENV", "qa")

    with pytest.raises(ValueError, match="BIOETL_ENV must be one of"):
        load_environment_settings()


@pytest.mark.unit
def test_apply_runtime_overrides_sets_nested_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PUBMED_TOOL", "bioetl-cli")
    monkeypatch.setenv("PUBMED_EMAIL", "contact@example.org")
    monkeypatch.setenv("PUBMED_API_KEY", "secret-key")
    monkeypatch.setenv("CROSSREF_MAILTO", "owner@example.org")
    monkeypatch.setenv("SEMANTIC_SCHOLAR_API_KEY", "semantic-secret")
    monkeypatch.setenv("IUPHAR_API_KEY", "iuphar-secret")

    settings = load_environment_settings()

    target_env: dict[str, str] = {}
    applied = apply_runtime_overrides(settings, environ=target_env)

    expected: dict[str, str] = {
        "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__TOOL": "bioetl-cli",
        "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__EMAIL": "contact@example.org",
        "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__API_KEY": "secret-key",
        "BIOETL__SOURCES__CROSSREF__IDENTIFY__MAILTO": "owner@example.org",
        "BIOETL__SOURCES__SEMANTIC_SCHOLAR__HTTP__HEADERS__X-API-KEY": "semantic-secret",
        "BIOETL__SOURCES__IUPHAR__HTTP__HEADERS__X-API-KEY": "iuphar-secret",
    }

    assert applied == expected
    assert target_env == expected


@pytest.mark.unit
def test_apply_runtime_overrides_preserves_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PUBMED_TOOL", "bioetl-cli")

    settings = load_environment_settings()

    existing_env: dict[str, str] = {
        "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__TOOL": "custom",
    }

    applied = apply_runtime_overrides(settings, environ=existing_env)

    assert "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__TOOL" not in applied
    assert existing_env["BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__TOOL"] == "custom"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("on", True),
        ("Off", False),
        (SecretStr("yes"), True),
        (SecretStr("0"), False),
    ],
)
def test_environment_settings_offline_chembl_client(raw: object, expected: bool) -> None:
    """The offline client flag relies on the shared boolean coercion."""

    settings = EnvironmentSettings(BIOETL_OFFLINE_CHEMBL_CLIENT=raw)

    assert settings.offline_chembl_client is expected

