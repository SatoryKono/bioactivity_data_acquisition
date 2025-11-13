"""CLI command ``bioetl-run-test-report``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.tools.run_test_report import TEST_REPORTS_ROOT
from bioetl.tools.run_test_report import (
    generate_test_report as generate_test_report_sync,
)

typer: Any = get_typer()

__all__ = ["app", "main", "run", "generate_test_report", "TEST_REPORTS_ROOT"]

generate_test_report = generate_test_report_sync

def main(
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

    try:
        exit_code = generate_test_report(output_root=output_root.resolve())
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    if exit_code == 0:
        typer.echo("Test report generated successfully")
    else:
        typer.secho(
            f"pytest exited with code {exit_code}",
            err=True,
            fg=typer.colors.RED,
        )
    exit_with_code(exit_code)


app: TyperApp = create_simple_tool_app(
    name="bioetl-run-test-report",
    help_text="Generate pytest and coverage reports with metadata",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

