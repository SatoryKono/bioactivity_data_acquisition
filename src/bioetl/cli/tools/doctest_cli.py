"""CLI command ``bioetl-doctest-cli``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.tools.doctest_cli import (
    CLIExample,
    CLIExampleResult,
    extract_cli_examples,
)
from bioetl.tools.doctest_cli import (
    run_examples as run_examples_sync,
)

typer: Any = get_typer()

__all__ = [
    "app",
    "main",
    "run",
    "run_examples",
    "extract_cli_examples",
    "CLIExample",
    "CLIExampleResult",
]

run_examples = run_examples_sync

def main() -> None:
    """Execute CLI doctests and analyze outcomes."""

    try:
        results, report_path = run_examples()
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    failed = [item for item in results if item.exit_code != 0]
    if failed:
        typer.secho(
            f"Not all CLI examples succeeded ({len(failed)} of {len(results)}).",
            err=True,
            fg=typer.colors.RED,
        )
        typer.echo(f"Report available at: {report_path.resolve()}")
        exit_with_code(1)

    typer.echo(
        f"All {len(results)} CLI examples completed successfully. "
        f"Report: {report_path.resolve()}"
    )
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-doctest-cli",
    help_text="Execute CLI examples and generate a report",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

