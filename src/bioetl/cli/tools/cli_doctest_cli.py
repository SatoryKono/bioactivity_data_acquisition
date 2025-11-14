"""CLI command ``bioetl-doctest-cli``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_doctest_cli as cli_doctest_cli_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL

CLIExample = cli_doctest_cli_impl.CLIExample
extract_cli_examples = cli_doctest_cli_impl.extract_cli_examples
run_examples = cli_doctest_cli_impl.run_examples

__all__ = (
    "CLIExample",
    "extract_cli_examples",
    "run_examples",
    "app",
    "cli_main",
    "run",
)

typer: Any = get_typer()


def cli_main() -> None:
    """Execute CLI doctests and analyze outcomes."""

    try:
        results, report_path = run_examples()
    except typer.Exit:
        raise
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"CLI doctest execution failed: {exc}",
            context={
                "command": "bioetl-doctest-cli",
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    failed = [item for item in results if item.exit_code != 0]
    if failed:
        typer.secho(
            f"Not all CLI examples succeeded ({len(failed)} of {len(results)}).",
            err=True,
            fg=typer.colors.RED,
        )
        typer.echo(f"Report available at: {report_path.resolve()}")
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=(
                f"CLI doctest failures detected ({len(failed)} of {len(results)}). "
                f"Report: {report_path.resolve()}"
            ),
            context={
                "command": "bioetl-doctest-cli",
                "failed_examples": len(failed),
                "total_examples": len(results),
                "report_path": str(report_path.resolve()),
            },
            exit_code=1,
        )

    typer.echo(
        f"All {len(results)} CLI examples completed successfully. "
        f"Report: {report_path.resolve()}"
    )
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-doctest-cli",
    help_text="Execute CLI examples and generate a report",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()