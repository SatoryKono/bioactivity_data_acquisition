from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from bioetl.cli.tools import dup_finder as dup_cli


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_cli_rejects_invalid_format(runner: CliRunner, tmp_path: Path) -> None:
    result = runner.invoke(
        dup_cli.app,
        [
            "--root",
            str(tmp_path),
            "--out",
            str(tmp_path / "artifacts"),
            "--format",
            "md,txt",
        ],
    )
    assert result.exit_code == 1
    assert "Invalid formats" in result.output


def test_cli_handles_runtime_error(runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def _raise(*_: Any, **__: Any) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(dup_cli, "run_dup_finder_workflow", _raise)
    result = runner.invoke(
        dup_cli.app,
        [
            "--root",
            str(tmp_path),
            "--out",
            str(tmp_path / "reports"),
        ],
    )
    assert result.exit_code == 1
    assert "Duplicate finder failed" in result.output


def test_cli_passes_arguments(runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, Any] = {}

    def _capture(*, root: Path, out: Path | None, fmt: str) -> None:
        captured["root"] = root
        captured["out"] = out
        captured["fmt"] = fmt

    monkeypatch.setattr(dup_cli, "run_dup_finder_workflow", _capture)
    result = runner.invoke(
        dup_cli.app,
        [
            "--root",
            str(tmp_path),
            "--out",
            "-",
            "--format",
            "md",
        ],
    )
    assert result.exit_code == 0
    assert captured["root"] == tmp_path
    assert captured["out"] == Path("-")
    assert captured["fmt"] == "md"

