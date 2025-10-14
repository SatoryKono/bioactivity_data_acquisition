"""Tests for the Typer-based CLI."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import responses
import yaml
from typer.testing import CliRunner

from src.library.cli import app


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
                "sources": [
                    {
                        "name": "chembl",
                        "base_url": "https://example.com",
                        "activities_endpoint": "/activities",
                        "page_size": 2,
                    }
                ],
                "output": {
                    "output_path": str(output_path),
                    "qc_report_path": str(qc_path),
                    "correlation_path": str(corr_path),
                },
                "retries": {"max_tries": 2},
                "log_level": "INFO",
                "strict_validation": True,
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
            "activities": [
                {
                    "assay_id": 1,
                    "molecule_chembl_id": "CHEMBL1",
                    "standard_value": 1.0,
                    "standard_units": "nM",
                    "activity_comment": None,
                }
            ],
            "next_page": False,
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
    assert frame.iloc[0]["standard_units"] == "nM"
    assert qc_path.exists()
    assert corr_path.exists()