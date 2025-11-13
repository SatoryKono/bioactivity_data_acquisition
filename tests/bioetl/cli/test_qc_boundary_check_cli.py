from __future__ import annotations

from pathlib import Path

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import qc_boundary_check as qc_boundary_check_cli
from bioetl.cli.tools.qc_boundary import Violation


def test_qc_boundary_check_success(
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    monkeypatch.setattr(
        qc_boundary_check_cli,
        "collect_qc_boundary_violations",
        lambda: [],
    )

    result = runner.invoke(qc_boundary_check_cli.app, [])

    assert result.exit_code == 0
    assert "CLI↔QC boundary is respected" in result.stdout


def test_qc_boundary_check_failure(
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
    tmp_path: Path,
) -> None:
    fake_path = tmp_path / "fake.py"
    fake_path.write_text("", encoding="utf-8")
    violation = Violation(
        chain=("bioetl.cli.fake", "bioetl.qc.helpers"),
        source_path=fake_path,
    )
    monkeypatch.setattr(
        qc_boundary_check_cli,
        "collect_qc_boundary_violations",
        lambda: [violation],
    )

    result = runner.invoke(qc_boundary_check_cli.app, [])

    assert result.exit_code == 1
    assert "CLI↔QC boundary violations detected" in result.stderr
    assert "fake.py" in result.stderr


