from __future__ import annotations

from pytest import MonkeyPatch
from typer.testing import CliRunner

from tools import check_output_artifacts as check_output_cli


def test_check_output_artifacts_success(
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    captured: dict[str, int] = {}

    def fake_check_output_artifacts(*, max_bytes: int) -> list[str]:
        captured["max_bytes"] = max_bytes
        return []

    monkeypatch.setattr(check_output_cli, "check_output_artifacts", fake_check_output_artifacts)

    result = runner.invoke(
        check_output_cli.app,
        ["--max-bytes", "2048"],
    )

    assert result.exit_code == 0
    assert "Каталог data/output чистый" in result.stdout
    assert captured["max_bytes"] == 2048


def test_check_output_artifacts_failure(
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_check_output_artifacts(*, max_bytes: int) -> list[str]:  # noqa: ARG001
        raise RuntimeError("unexpected artifact")

    monkeypatch.setattr(check_output_cli, "check_output_artifacts", fake_check_output_artifacts)

    result = runner.invoke(check_output_cli.app, [])

    assert result.exit_code == 1
    assert "unexpected artifact" in result.stderr


