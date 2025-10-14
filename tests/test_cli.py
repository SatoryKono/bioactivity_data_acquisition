"""Tests for the Typer-based CLI."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from library.cli import app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def sample_config(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    config_path = tmp_path / "config.yaml"
    data_path = tmp_path / "output.csv"
    qc_path = tmp_path / "qc.csv"
    corr_path = tmp_path / "corr.csv"
    config_path.write_text(
        yaml.safe_dump(
            {
                "clients": [
                    {
                        "name": "chembl",
                        "url": "https://example.com/api",
                        "pagination_param": None,
                    }
                ],
                "output": {
                    "data_path": str(data_path),
                    "qc_report_path": str(qc_path),
                    "correlation_path": str(corr_path),
                },
                "retries": {"max_tries": 1, "backoff_multiplier": 1.0},
            }
        ),
        encoding="utf-8",
    )
    return config_path, data_path, qc_path, corr_path


@pytest.fixture(autouse=True)
def mock_client(monkeypatch: pytest.MonkeyPatch) -> None:
    def _mock_fetch(self) -> list[dict[str, object]]:
        return [
            {
                "compound_id": "CHEMBL1",
                "target_pref_name": "BRAF",
                "activity_value": 1.0,
                "activity_units": "nM",
                "source": "chembl",
                "retrieved_at": "2024-01-01T00:00:00Z",
                "smiles": "CCO",
            }
        ]

    monkeypatch.setattr("library.clients.bioactivity.BioactivityClient.fetch_records", _mock_fetch)


def test_pipeline_command_writes_outputs(
    runner: CliRunner, sample_config: tuple[Path, Path, Path, Path]
) -> None:
    config_path, data_path, qc_path, corr_path = sample_config

    result = runner.invoke(app, ["pipeline", "--config", str(config_path)])
    assert result.exit_code == 0, result.output

    assert data_path.exists()
    df = pd.read_csv(data_path)
    assert df.shape[0] == 1
    assert set(df.columns) == {
        "compound_id",
        "target",
        "activity_value",
        "activity_unit",
        "source",
        "retrieved_at",
        "smiles",
    }
    assert qc_path.exists()
    assert corr_path.exists()


