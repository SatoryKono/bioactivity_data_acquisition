"""Tests for the Typer-based CLI."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import responses
import yaml
from typer.testing import CliRunner

from library.cli import app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def sample_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    output_path = tmp_path / "bioactivities.csv"
    qc_path = tmp_path / "qc.csv"
    corr_path = tmp_path / "corr.csv"

    config_path.write_text(
        yaml.safe_dump(
            {
                "http": {
                    "global": {
                        "timeout": 5,
                        "retries": {"max_tries": 2, "backoff_multiplier": 1.0},
                        "headers": {"User-Agent": "pytest"},
                    }
                },
                "sources": {
                    "chembl": {
                        "name": "chembl",
                        "endpoint": "activities",
                        "pagination": {
                            "page_param": "page",
                            "size_param": "page_size",
                            "size": 50,
                            "max_pages": 1,
                        },
                        "http": {
                            "base_url": "https://example.com",
                            "headers": {"Accept": "application/json"},
                        },
                    }
                },
                "io": {
                    "output": {
                        "data_path": str(output_path),
                        "qc_report_path": str(qc_path),
                        "correlation_path": str(corr_path),
                        "format": "csv",
                        "csv": {"encoding": "utf-8", "float_format": "%.6f"},
                    }
                },
                "determinism": {
                    "sort": {
                        "by": ["compound_id", "target"],
                        "ascending": [True, True],
                        "na_position": "last",
                    },
                    "column_order": [
                        "compound_id",
                        "target",
                        "activity_value",
                        "activity_unit",
                        "source",
                        "retrieved_at",
                        "smiles",
                    ],
                },
                "transforms": {
                    "unit_conversion": {"nM": 1.0, "uM": 1000.0, "pM": 0.001}
                },
                "logging": {"level": "INFO"},
                "validation": {
                    "strict": True,
                    "qc": {
                        "max_missing_fraction": 1.0,
                        "max_duplicate_fraction": 1.0,
                    },
                },
                "postprocess": {"qc": {"enabled": True}, "correlation": {"enabled": True}},
            }
        ),
        encoding="utf-8",
    )
    return config_path


@responses.activate
def test_cli_pipeline_command(runner: CliRunner, sample_config: Path) -> None:
    """Test that the CLI pipeline command works correctly."""
    responses.add(
        responses.GET,
        "https://example.com/activities",
        json={
            "results": [
                {
                    "compound_id": "CHEMBL1",
                    "target_pref_name": "Protein X",
                    "activity_value": 1.0,
                    "activity_units": "uM",
                    "source": "chembl",
                    "retrieved_at": "2024-01-01T00:00:00Z",
                    "smiles": "C1=CC=CC=C1",
                }
            ]
        },
    )

    result = runner.invoke(app, ["pipeline", "--config", str(sample_config)])
    assert result.exit_code == 0
    
    # Check that output files were created
    output_path = sample_config.parent / "bioactivities.csv"
    qc_path = sample_config.parent / "qc.csv"
    corr_path = sample_config.parent / "corr.csv"
    
    assert output_path.exists()
    frame = pd.read_csv(output_path)
    assert list(frame.columns) == [
        "compound_id",
        "target",
        "activity_value",
        "activity_unit",
        "source",
        "retrieved_at",
        "smiles",
    ]
    assert frame.loc[0, "activity_unit"] == "nM"
    assert frame.loc[0, "activity_value"] == pytest.approx(1000.0, rel=1e-6)
    assert qc_path.exists()
    assert corr_path.exists()


def test_cli_invalid_override_format(runner: CliRunner, sample_config: Path) -> None:
    """Invalid override syntax should surface a helpful error."""

    result = runner.invoke(
        app, ["pipeline", "--config", str(sample_config), "--set", "not-a-valid-override"]
    )

    assert result.exit_code == 2
    assert "Overrides must be in KEY=VALUE format" in result.stderr
