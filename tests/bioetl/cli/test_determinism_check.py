from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import determinism_check as determinism_cli


def test_determinism_check_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    report_path = tmp_path / "determinism.html"

    def fake_run_determinism_check(*, pipelines: tuple[str, ...] | None) -> dict[str, SimpleNamespace]:
        assert pipelines is None
        return {
            "activity_chembl": SimpleNamespace(deterministic=True, report_path=report_path),
        }

    monkeypatch.setattr(determinism_cli, "run_determinism_check", fake_run_determinism_check)

    result = runner.invoke(determinism_cli.app, [])

    assert result.exit_code == 0
    assert "All inspected pipelines are deterministic" in result.stdout


def test_determinism_check_failure_no_results(
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_run_determinism_check(*, pipelines: tuple[str, ...] | None) -> dict[str, SimpleNamespace]:
        return {}

    monkeypatch.setattr(determinism_cli, "run_determinism_check", fake_run_determinism_check)

    result = runner.invoke(determinism_cli.app, [])

    assert result.exit_code == 1
    assert "No pipelines found for determinism check" in result.stderr


