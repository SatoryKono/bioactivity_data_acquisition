"""Tests for configuration loading and precedence rules."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from bioactivity.config import Config


@pytest.fixture()
def config_yaml(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    output_dir = tmp_path / "outputs"
    output = {
        "io": {
            "output": {
                "data_path": str(output_dir / "bioactivities.csv"),
                "qc_report_path": str(output_dir / "qc.csv"),
                "correlation_path": str(output_dir / "corr.csv"),
            }
        },
        "sources": {
            "chembl": {
                "name": "chembl",
                "url": "https://example.org/activities",
                "pagination": {"max_pages": 1},
                "auth": {"secret_name": "chembl_api_token"},
            }
        },
        "secrets": {
            "required": [
                {
                    "name": "chembl_api_token",
                    "env": "CHEMBL_TOKEN",
                    "description": "Token for tests",
                }
            ]
        },
    }
    config_path.write_text(yaml.safe_dump(output), encoding="utf-8")
    return config_path


def test_load_applies_defaults_and_secrets(monkeypatch: pytest.MonkeyPatch, config_yaml: Path) -> None:
    """Loading a config merges defaults and resolves secrets."""

    monkeypatch.setenv("CHEMBL_TOKEN", "s3cr3t")
    loaded = Config.load(config_yaml)
    assert loaded.runtime.log_level == "INFO"
    assert loaded.clients[0].headers["Authorization"] == "Bearer s3cr3t"


def test_environment_overrides_take_precedence(monkeypatch: pytest.MonkeyPatch, config_yaml: Path) -> None:
    """Environment variables override YAML values."""

    monkeypatch.setenv("CHEMBL_TOKEN", "token")
    monkeypatch.setenv("BIOACTIVITY__RUNTIME__LOG_LEVEL", "DEBUG")
    loaded = Config.load(config_yaml)
    assert loaded.runtime.log_level == "DEBUG"


def test_cli_overrides_win_over_environment(monkeypatch: pytest.MonkeyPatch, config_yaml: Path) -> None:
    """CLI overrides have the highest priority."""

    monkeypatch.setenv("CHEMBL_TOKEN", "token")
    monkeypatch.setenv("BIOACTIVITY__RUNTIME__LOG_LEVEL", "DEBUG")
    overrides = {"runtime.log_level": "WARNING"}
    loaded = Config.load(config_yaml, cli_overrides=overrides)
    assert loaded.runtime.log_level == "WARNING"


def test_missing_required_secret_raises(monkeypatch: pytest.MonkeyPatch, config_yaml: Path) -> None:
    """Missing required secrets result in a validation error."""

    monkeypatch.delenv("CHEMBL_TOKEN", raising=False)
    with pytest.raises(ValueError, match="Missing required secrets"):
        Config.load(config_yaml)


def test_parse_cli_overrides_errors() -> None:
    """Invalid CLI override syntax raises a helpful error."""

    with pytest.raises(ValueError, match="KEY=VALUE"):
        Config.parse_cli_overrides(["runtime.log_level"])
