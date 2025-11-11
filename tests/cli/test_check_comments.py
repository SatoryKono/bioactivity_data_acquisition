from __future__ import annotations

from pathlib import Path

from pytest import MonkeyPatch
from typer.testing import CliRunner

from tools import check_comments as check_comments_cli


def test_check_comments_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    captured: dict[str, Path | None] = {}

    def fake_run_comment_check(*, root: Path | None) -> None:
        captured["root"] = root

    monkeypatch.setattr(check_comments_cli, "run_comment_check", fake_run_comment_check)

    project_root = tmp_path / "repo"
    project_root.mkdir()
    result = runner.invoke(
        check_comments_cli.app,
        ["--root", str(project_root)],
    )

    assert result.exit_code == 0
    assert "Проверка комментариев завершена" in result.stdout
    assert captured["root"] == project_root.resolve()


def test_check_comments_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_run_comment_check(*, root: Path | None) -> None:  # noqa: ARG001
        raise NotImplementedError("comment audit disabled")

    monkeypatch.setattr(check_comments_cli, "run_comment_check", fake_run_comment_check)

    result = runner.invoke(
        check_comments_cli.app,
        ["--root", str(tmp_path)],
    )

    assert result.exit_code == 1
    assert "comment audit disabled" in result.stderr


