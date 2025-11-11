from __future__ import annotations

from pathlib import Path

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import remove_type_ignore as remove_type_ignore_cli


def test_remove_type_ignore_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_remove_type_ignore(*, root: Path | None) -> int:
        return 5

    monkeypatch.setattr(remove_type_ignore_cli, "remove_type_ignore", fake_remove_type_ignore)

    result = runner.invoke(
        remove_type_ignore_cli.app,
        ["--root", str(tmp_path)],
    )

    assert result.exit_code == 0
    assert "Удалено директив `type: ignore`: 5" in result.stdout


def test_remove_type_ignore_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_remove_type_ignore(*, root: Path | None) -> int:  # noqa: ARG001
        raise RuntimeError("type ignore removal failed")

    monkeypatch.setattr(remove_type_ignore_cli, "remove_type_ignore", fake_remove_type_ignore)

    result = runner.invoke(
        remove_type_ignore_cli.app,
        ["--root", str(tmp_path)],
    )

    assert result.exit_code == 1
    assert "type ignore removal failed" in result.stderr


