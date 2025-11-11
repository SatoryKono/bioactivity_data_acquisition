from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from tools import audit_docs as audit_docs_cli


def test_audit_docs_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
) -> None:
    captured: dict[str, Path] = {}

    def fake_run_audit(*, artifacts_dir: Path) -> None:
        captured["artifacts_dir"] = artifacts_dir

    monkeypatch.setattr(audit_docs_cli, "run_audit", fake_run_audit)

    target_dir = tmp_path / "reports"
    result = runner.invoke(
        audit_docs_cli.app,
        ["--artifacts", str(target_dir)],
    )

    assert result.exit_code == 0
    assert "Аудит завершён" in result.stdout
    assert captured["artifacts_dir"] == target_dir.resolve()


def test_audit_docs_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_run_audit(*, artifacts_dir: Path) -> None:  # noqa: ARG001
        typer_module = audit_docs_cli.typer
        typer_module.secho("audit failure", err=True, fg=typer_module.colors.RED)
        raise typer_module.Exit(code=1)

    monkeypatch.setattr(audit_docs_cli, "run_audit", fake_run_audit)

    target_dir = tmp_path / "reports"
    result = runner.invoke(
        audit_docs_cli.app,
        ["--artifacts", str(target_dir)],
    )

    assert result.exit_code == 1
    assert "audit failure" in result.stderr


