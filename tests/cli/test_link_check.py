from __future__ import annotations

from pytest import MonkeyPatch
from typer.testing import CliRunner

from tools import link_check as link_check_cli


def test_link_check_success(
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_run_link_check(*, timeout_seconds: int) -> int:
        assert timeout_seconds == 300
        return 0

    monkeypatch.setattr(link_check_cli, "run_link_check", fake_run_link_check)

    result = runner.invoke(link_check_cli.app, [])

    assert result.exit_code == 0
    assert "Проверка ссылок завершена успешно" in result.stdout


def test_link_check_failure(
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_run_link_check(*, timeout_seconds: int) -> int:  # noqa: ARG001
        return 2

    monkeypatch.setattr(link_check_cli, "run_link_check", fake_run_link_check)

    result = runner.invoke(link_check_cli.app, [])

    assert result.exit_code == 2
    assert "завершилась с ошибками" in result.stderr


