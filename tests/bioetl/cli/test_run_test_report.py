from __future__ import annotations

from pathlib import Path

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import run_test_report as test_report_cli


def test_run_test_report_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_generate_test_report(*, output_root: Path) -> int:
        assert output_root == (tmp_path / "reports").resolve()
        return 0

    monkeypatch.setattr(test_report_cli, "generate_test_report", fake_generate_test_report)

    result = runner.invoke(
        test_report_cli.app,
        ["--output-root", str(tmp_path / "reports")],
    )

    assert result.exit_code == 0
    assert "Тестовый отчёт сформирован успешно" in result.stdout


def test_run_test_report_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_generate_test_report(*, output_root: Path) -> int:  # noqa: ARG001
        return 3

    monkeypatch.setattr(test_report_cli, "generate_test_report", fake_generate_test_report)

    result = runner.invoke(
        test_report_cli.app,
        ["--output-root", str(tmp_path / "reports")],
    )

    assert result.exit_code == 3
    assert "pytest завершился с кодом 3" in result.stderr


