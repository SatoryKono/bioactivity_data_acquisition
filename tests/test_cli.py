"""Tests for the Typer-based CLI."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import responses
import yaml
from typer.testing import CliRunner

from bioactivity.cli import app


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
                "clients": [
                    {
                        "name": "chembl",
                        "url": "https://example.com/activities",
                        "params": {},
                        "pagination_param": "page",
                        "page_size_param": "page_size",
                        "page_size": 50,
                        "max_pages": 1,
                    }
                ],
                "output": {
                    "data_path": str(output_path),
                    "qc_report_path": str(qc_path),
                    "correlation_path": str(corr_path),
                },
                "retries": {"max_tries": 2, "backoff_multiplier": 1.0},
                "logging": {"level": "INFO"},
                "validation": {"strict": True},
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
        "activity_unit",
        "activity_value",
        "compound_id",
        "retrieved_at",
        "smiles",
        "source",
        "target",
    ]
    assert frame.loc[0, "activity_unit"] == "nM"
    assert frame.loc[0, "activity_value"] == pytest.approx(1000.0, rel=1e-6)
    assert qc_path.exists()
    assert corr_path.exists()