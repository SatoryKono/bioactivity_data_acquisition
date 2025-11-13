from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import create_matrix_doc_code as matrix_cli


def test_create_matrix_doc_code_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_write_matrix(*, artifacts_dir: Path) -> SimpleNamespace:
        return SimpleNamespace(
            rows=[{"doc": "README.md", "module": "bioetl"}],
            csv_path=artifacts_dir / "matrix.csv",
            json_path=artifacts_dir / "matrix.json",
        )

    monkeypatch.setattr(matrix_cli, "write_matrix", fake_write_matrix)

    artifacts_dir = tmp_path / "matrix"
    result = runner.invoke(matrix_cli.app, ["--artifacts", str(artifacts_dir)])

    assert result.exit_code == 0
    assert "Matrix with 1 rows saved to" in result.stdout


def test_create_matrix_doc_code_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_write_matrix(*, artifacts_dir: Path) -> SimpleNamespace:  # noqa: ARG001
        raise RuntimeError("matrix error")

    monkeypatch.setattr(matrix_cli, "write_matrix", fake_write_matrix)

    result = runner.invoke(
        matrix_cli.app,
        ["--artifacts", str(tmp_path / "matrix")],
    )

    assert result.exit_code == 1
    assert "matrix error" in result.stderr


