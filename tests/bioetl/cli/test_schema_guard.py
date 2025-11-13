from __future__ import annotations

from pathlib import Path

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import schema_guard as schema_guard_cli


def test_schema_guard_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    report_path = tmp_path / "schema_guard_report.json"

    def fake_run_schema_guard() -> tuple[dict[str, dict[str, bool]], list[str], Path]:
        return (
            {"activity": {"valid": True}},
            [],
            report_path,
        )

    monkeypatch.setattr(schema_guard_cli, "run_schema_guard", fake_run_schema_guard)

    result = runner.invoke(schema_guard_cli.app, [])

    assert result.exit_code == 0
    assert "All configurations are valid" in result.stdout
    assert str(report_path.resolve()) in result.stdout


def test_schema_guard_failure_exception(
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_run_schema_guard() -> tuple[dict[str, dict[str, bool]], list[str], Path]:
        raise RuntimeError("schema registry error")

    monkeypatch.setattr(schema_guard_cli, "run_schema_guard", fake_run_schema_guard)

    result = runner.invoke(schema_guard_cli.app, [])

    assert result.exit_code == 1
    assert "schema registry error" in result.stderr


