from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import doctest_cli as doctest_cli_app


def test_doctest_cli_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    report_path = tmp_path / "cli_report.json"

    def fake_run_examples() -> tuple[list[SimpleNamespace], Path]:
        return [SimpleNamespace(exit_code=0)], report_path

    monkeypatch.setattr(doctest_cli_app, "run_examples", fake_run_examples)

    result = runner.invoke(doctest_cli_app.app, [])

    assert result.exit_code == 0
    assert "Все 1 CLI-примеров выполнены успешно" in result.stdout


def test_doctest_cli_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    report_path = tmp_path / "cli_report.json"

    def fake_run_examples() -> tuple[list[SimpleNamespace], Path]:
        return [SimpleNamespace(exit_code=0), SimpleNamespace(exit_code=1)], report_path

    monkeypatch.setattr(doctest_cli_app, "run_examples", fake_run_examples)

    result = runner.invoke(doctest_cli_app.app, [])

    assert result.exit_code == 1
    assert "Не все CLI-примеры прошли успешно" in result.stderr
    assert str(report_path.resolve()) in result.stdout


