from __future__ import annotations

import logging
from pathlib import Path

from typer.testing import CliRunner

from interfaces.cli.common import create_app, register_commands, run_app
from interfaces.cli.decorators import common_options


def test_help_includes_common_options() -> None:
    runner = CliRunner()
    app = create_app("demo-cli")

    @common_options
    def main(config: Path | None, verbose: bool) -> None:  # noqa: ARG001 - параметры нужны Typer
        return None

    register_commands(app, [("run", main)])

    result = runner.invoke(app, ["run", "--help"])

    assert result.exit_code == 0
    assert "--config" in result.stdout
    assert "--verbose" in result.stdout


def test_run_app_logs_exceptions(caplog) -> None:
    app = create_app("demo-cli")

    @app.command()
    def fail() -> None:
        raise RuntimeError("boom")

    caplog.set_level(logging.ERROR, logger="interfaces.cli.common")
    exit_code = run_app(app, argv=["fail"])

    assert exit_code == 1
    assert any("CLI execution failed" in record.getMessage() for record in caplog.records)
