"""Smoke tests for the Typer CLI."""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest
from scripts.fetch_publications import app
from typer.testing import CliRunner


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def sample_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
logging:
  level: INFO
etl:
  strict_validation: true
sources:
  chembl:
    base_url: https://example.org/chembl
  pubmed:
    base_url: https://example.org/pubmed
        """.strip(),
        encoding="utf-8",
    )
    return config_path


@pytest.fixture()
def sample_input(tmp_path: Path) -> Path:
    csv_path = tmp_path / "queries.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["query", "type"])
        writer.writerow(["CHEMBL25", "chembl"])
    return csv_path


def test_run_command_creates_output_file(runner: CliRunner, sample_config: Path, sample_input: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "output.csv"
    result = runner.invoke(
        app,
        [
            "run",
            "--config",
            str(sample_config),
            "--input",
            str(sample_input),
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0, result.stdout
    frame = pd.read_csv(output_path)
    assert set(frame.columns) == {"source", "identifier", "title", "published_at", "doi"}


def test_extract_command_skips_writing_output(runner: CliRunner, sample_config: Path, sample_input: Path) -> None:
    result = runner.invoke(
        app,
        [
            "extract",
            "--config",
            str(sample_config),
            "--input",
            str(sample_input),
        ],
    )
    assert result.exit_code == 0, result.stdout
