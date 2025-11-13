from __future__ import annotations

from pathlib import Path

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import semantic_diff as semantic_diff_cli


def test_semantic_diff_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_run_semantic_diff() -> Path:
        return tmp_path / "diff_report.md"

    monkeypatch.setattr(semantic_diff_cli, "run_semantic_diff", fake_run_semantic_diff)

    result = runner.invoke(semantic_diff_cli.app, [])

    assert result.exit_code == 0
    assert "Semantic diff report written to" in result.stdout


def test_semantic_diff_failure(
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_run_semantic_diff() -> Path:
        raise RuntimeError("semantic diff failed")

    monkeypatch.setattr(semantic_diff_cli, "run_semantic_diff", fake_run_semantic_diff)

    result = runner.invoke(semantic_diff_cli.app, [])

    assert result.exit_code == 1
    assert "semantic diff failed" in result.stderr


