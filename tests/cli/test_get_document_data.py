"""Tests for the ``get-document-data`` CLI command."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from typer.testing import CliRunner

import library.cli as cli_module
from library.cli import ExitCode, app
from library.documents import pipeline as document_pipeline


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _write_input(path: Path) -> None:
    frame = pd.DataFrame(
        {
            "document_chembl_id": ["DOC1", "DOC2"],
            "doi": ["10.1000/test1", "10.1000/test2"],
            "title": ["First", "Second"],
        }
    )
    frame.to_csv(path, index=False)


def test_invalid_date_tag_triggers_validation_error(tmp_path: Path, runner: CliRunner) -> None:
    input_csv = tmp_path / "input.csv"
    _write_input(input_csv)

    result = runner.invoke(
        app,
        [
            "get-document-data",
            "--documents-csv",
            str(input_csv),
            "--output-dir",
            str(tmp_path / "out"),
            "--date-tag",
            "20231",
            "--dry-run",
        ],
    )

    assert result.exit_code == ExitCode.VALIDATION_ERROR
    assert "validation error" in result.output.lower()


def test_cli_precedence_prefers_cli_over_env_and_yaml(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, runner: CliRunner
) -> None:
    input_csv = tmp_path / "input.csv"
    _write_input(input_csv)

    config_path = tmp_path / "config.yaml"
    config_path.write_text("runtime:\n  workers: 2\n")

    recorded: dict[str, Any] = {}

    def fake_run(config: Any, frame: pd.DataFrame) -> document_pipeline.DocumentETLResult:
        recorded["workers"] = config.runtime.workers
        return document_pipeline.DocumentETLResult(documents=frame, qc=pd.DataFrame())

    monkeypatch.setattr(cli_module, "run_document_etl", fake_run)
    monkeypatch.setenv("BIOACTIVITY__RUNTIME__WORKERS", "6")

    result = runner.invoke(
        app,
        [
            "get-document-data",
            "--config",
            str(config_path),
            "--documents-csv",
            str(input_csv),
            "--output-dir",
            str(tmp_path / "out"),
            "--workers",
            "4",
            "--dry-run",
        ],
    )

    assert result.exit_code == ExitCode.OK
    assert recorded["workers"] == 4


def test_dry_run_skips_output_writes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, runner: CliRunner) -> None:
    input_csv = tmp_path / "input.csv"
    _write_input(input_csv)

    writes: list[Path] = []

    def fake_write(
        result: document_pipeline.DocumentETLResult, output_dir: Path, date_tag: str
    ) -> dict[str, Path]:
        writes.append(output_dir / f"documents_{date_tag}.csv")
        return {
            "documents": output_dir / f"documents_{date_tag}.csv",
            "qc": output_dir / f"documents_{date_tag}_qc.csv",
        }

    monkeypatch.setattr(cli_module, "write_document_outputs", fake_write)

    result = runner.invoke(
        app,
        [
            "get-document-data",
            "--documents-csv",
            str(input_csv),
            "--output-dir",
            str(tmp_path / "out"),
            "--dry-run",
        ],
    )

    assert result.exit_code == ExitCode.OK
    assert writes == []


def test_qc_failure_returns_expected_exit_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, runner: CliRunner
) -> None:
    input_csv = tmp_path / "input.csv"
    _write_input(input_csv)

    def fail_run(config: Any, frame: pd.DataFrame) -> document_pipeline.DocumentETLResult:
        raise document_pipeline.DocumentQCError("synthetic qc failure")

    monkeypatch.setattr(cli_module, "run_document_etl", fail_run)

    result = runner.invoke(
        app,
        [
            "get-document-data",
            "--documents-csv",
            str(input_csv),
            "--output-dir",
            str(tmp_path / "out"),
            "--dry-run",
        ],
    )

    assert result.exit_code == ExitCode.QC_ERROR
    assert "synthetic qc failure" in result.output
