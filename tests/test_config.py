"""Tests for configuration loading and precedence rules."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from library.config import Config


@pytest.fixture()
def config_yaml(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    output_dir = tmp_path / "outputs"
    input_docs = tmp_path / "inputs" / "documents.csv"
    output = {
        "http": {
            "global": {
                "timeout_sec": 30,
                "retries": {"total": 3},
                "headers": {"User-Agent": "test"},
            }
        },
        "io": {
            "input": {"documents_csv": str(input_docs)},
            "output": {
                "data_path": str(output_dir / "bioactivities.csv"),
                "qc_report_path": str(output_dir / "qc.csv"),
                "correlation_path": str(output_dir / "corr.csv"),
            }
        },
        "runtime": {"workers": 2},
        "logging": {"level": "INFO"},
        "sources": {
            "chembl": {
                "name": "chembl",
                "enabled": True,
                "endpoint": "activities",
                "pagination": {"max_pages": 1},
                "http": {
                    "base_url": "https://example.org/activities",
                    "headers": {"Authorization": "Bearer {CHEMBL_TOKEN}"},
                },
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
    assert loaded.logging.level == "INFO"
    assert loaded.clients[0].headers["Authorization"] == "Bearer s3cr3t"


def test_environment_overrides_take_precedence(monkeypatch: pytest.MonkeyPatch, config_yaml: Path) -> None:
    """Environment variables override YAML values."""

    monkeypatch.setenv("CHEMBL_TOKEN", "token")
    monkeypatch.setenv("BIOACTIVITY__LOGGING__LEVEL", "DEBUG")
    loaded = Config.load(config_yaml)
    assert loaded.logging.level == "DEBUG"


def test_cli_overrides_win_over_environment(monkeypatch: pytest.MonkeyPatch, config_yaml: Path) -> None:
    """CLI overrides have the highest priority."""

    monkeypatch.setenv("CHEMBL_TOKEN", "token")
    monkeypatch.setenv("BIOACTIVITY__LOGGING__LEVEL", "DEBUG")
    overrides = {"logging.level": "WARNING"}
    loaded = Config.load(config_yaml, overrides=overrides)
    assert loaded.logging.level == "WARNING"


def test_missing_required_secret_raises(monkeypatch: pytest.MonkeyPatch, config_yaml: Path) -> None:
    """Missing required secrets result in a validation error."""

    monkeypatch.delenv("CHEMBL_TOKEN", raising=False)
    # Config should raise an error when trying to access clients with missing secrets
    loaded = Config.load(config_yaml)
    with pytest.raises(ValueError, match="Missing required environment variables: CHEMBL_TOKEN"):
        _ = loaded.clients


def test_parse_cli_overrides_errors() -> None:
    """Invalid CLI override syntax raises a helpful error."""

    with pytest.raises(ValueError, match="KEY=VALUE"):
        Config.parse_cli_overrides(["logging.level"])
