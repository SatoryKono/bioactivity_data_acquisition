"""CLI command ``bioetl-run-test-report``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_run_test_report as cli_run_test_report_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL

_LOGIC_EXPORTS = getattr(cli_run_test_report_impl, "__all__", [])
globals().update(
    {symbol: getattr(cli_run_test_report_impl, symbol) for symbol in _LOGIC_EXPORTS}
)
__all__ = [* _LOGIC_EXPORTS, "app", "cli_main", "run"]

typer: Any = get_typer()
TEST_REPORTS_ROOT = cli_run_test_report_impl.TEST_REPORTS_ROOT


def cli_main(
    output_root: Path = typer.Option(
        TEST_REPORTS_ROOT,
        "--output-root",
        help="Directory where pytest and coverage artifacts will be stored.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
) -> None:
    """Run pytest and build the combined report."""

    output_root_path = output_root.resolve()
    try:
        exit_code = cli_run_test_report_impl.generate_test_report(output_root=output_root_path)
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Test report generation failed: {exc}",
            context={
                "command": "bioetl-run-test-report",
                "output_root": str(output_root_path),
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    if exit_code == 0:
        typer.echo("Test report generated successfully")
    else:
        typer.secho(
            f"pytest exited with code {exit_code}",
            err=True,
            fg=typer.colors.RED,
        )
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"pytest exited with code {exit_code}",
            context={
                "command": "bioetl-run-test-report",
                "output_root": str(output_root_path),
                "pytest_exit_code": exit_code,
            },
        )
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-run-test-report",
    help_text="Generate pytest and coverage reports with metadata",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()