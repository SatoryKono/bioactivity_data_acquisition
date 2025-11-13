from __future__ import annotations

from pathlib import Path

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import qc_boundary_check as qc_boundary_check_cli
from bioetl.pipelines.qc.boundary_check import (
    QCBoundaryReport,
    QCBoundaryViolation,
)


def test_qc_boundary_check_success(
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    monkeypatch.setattr(
        qc_boundary_check_cli,
        "collect_cli_qc_boundary_report",
        lambda: QCBoundaryReport(package="bioetl.cli", violations=()),
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
    monkeypatch.setattr(
        qc_boundary_check_cli,
        "collect_cli_qc_boundary_report",
        lambda: QCBoundaryReport(
            package="bioetl.cli",
            violations=(
                QCBoundaryViolation(
                    module="bioetl.cli.fake",
                    qc_module="bioetl.qc.helpers",
                    import_chain=("bioetl.cli.fake", "bioetl.qc.helpers"),
                    source_path=fake_path,
                ),
            ),
        ),
    )

    result = runner.invoke(qc_boundary_check_cli.app, [])

    assert result.exit_code == 1
    assert "CLI↔QC boundary violations detected" in result.stderr
    assert "fake.py" in result.stderr


