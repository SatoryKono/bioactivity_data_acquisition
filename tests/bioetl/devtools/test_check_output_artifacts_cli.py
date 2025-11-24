from __future__ import annotations

import pytest
from typer.testing import CliRunner

from bioetl.devtools import cli_check_output_artifacts


def test_cli_invokes_wrapper(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    invoked: dict[str, int] = {}

    def _fake_check(max_bytes: int) -> list[str]:
        invoked["max_bytes"] = max_bytes
        return []

    monkeypatch.setattr(cli_check_output_artifacts, "check_output_artifacts", _fake_check)

    result = runner.invoke(cli_check_output_artifacts.app, [])

    assert result.exit_code == 0
    assert "data/output directory is clean" in result.stdout
    assert invoked["max_bytes"] == cli_check_output_artifacts.MAX_BYTES


def test_cli_reports_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()

    def _fake_check(max_bytes: int) -> list[str]:
        assert max_bytes == cli_check_output_artifacts.MAX_BYTES
        return ["Tracked artifacts detected", "Oversized artifact"]

    monkeypatch.setattr(cli_check_output_artifacts, "check_output_artifacts", _fake_check)

    result = runner.invoke(cli_check_output_artifacts.app, [])

    assert result.exit_code == 1
    assert "Tracked artifacts detected" in result.stdout
    assert "Oversized artifact" in result.stdout
    assert "Found 2 output artifacts exceeding limits" in result.stderr
    assert "[bioetl-cli] ERROR" in result.stderr
