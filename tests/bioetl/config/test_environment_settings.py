"""Tests for env settings helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from bioetl.config.environment import (
    EnvironmentSettings,
    apply_runtime_overrides,
    build_env_override_mapping,
    load_environment_settings,
    resolve_env_layers,
)


@pytest.mark.unit
def test_load_environment_settings_defaults(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delenv("BIOETL_ENV", raising=False)
    monkeypatch.delenv("PUBMED_TOOL", raising=False)

    settings = load_environment_settings()

    assert settings.bioetl_env is None
    assert settings.pubmed_tool is None


@pytest.mark.unit
def test_load_environment_settings_invalid_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BIOETL_ENV", "qa")

    with pytest.raises(ValueError, match="BIOETL_ENV must be one of"):
        load_environment_settings()


@pytest.mark.unit
def test_apply_runtime_overrides_sets_nested_keys(monkeypatch: MonkeyPatch) -> None:
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
def test_apply_runtime_overrides_preserves_existing(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("PUBMED_TOOL", "bioetl-cli")

    settings = load_environment_settings()

    existing_env: dict[str, str] = {
        "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__TOOL": "custom",
    }

    applied = apply_runtime_overrides(settings, environ=existing_env)

    assert "BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__TOOL" not in applied
    assert existing_env["BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__TOOL"] == "custom"


@pytest.mark.unit
def test_build_env_override_mapping_returns_nested() -> None:
    settings = EnvironmentSettings.model_validate(
        {
            "pubmed_tool": "bioetl-cli",
            "pubmed_email": "ops@example.org",
        }
    )

    overrides = build_env_override_mapping(settings)

    pubmed_identify = overrides["sources"]["pubmed"]["http"]["identify"]
    assert pubmed_identify["tool"] == "bioetl-cli"
    assert pubmed_identify["email"] == "ops@example.org"


@pytest.mark.unit
def test_resolve_env_layers_returns_sorted(tmp_path: Path) -> None:
    env_dir = tmp_path / "configs" / "env" / "dev"
    env_dir.mkdir(parents=True)
    layer_a = env_dir / "b.yaml"
    layer_b = env_dir / "a.yaml"
    layer_a.write_text("pipeline:\n  name: layer-a\n")
    layer_b.write_text("pipeline:\n  name: layer-b\n")

    files = resolve_env_layers("dev", base=tmp_path)

    relative = [str(path.relative_to(tmp_path)).replace("\\", "/") for path in files]
    assert relative == ["configs/env/dev/a.yaml", "configs/env/dev/b.yaml"]


@pytest.mark.unit
def test_resolve_env_layers_none(tmp_path: Path) -> None:
    assert resolve_env_layers(None, base=tmp_path) == []


@pytest.mark.unit
def test_resolve_env_layers_missing_directory(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        resolve_env_layers("prod", base=tmp_path)
