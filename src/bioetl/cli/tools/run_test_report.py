"""CLI command ``bioetl-run-test-report``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.tools import emit_tool_error, exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL
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

    output_root_path = output_root.resolve()
    try:
        exit_code = generate_test_report(output_root=output_root_path)
    except Exception as exc:  # noqa: BLE001
        emit_tool_error(
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
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=f"pytest exited with code {exit_code}",
            context={
                "command": "bioetl-run-test-report",
                "output_root": str(output_root_path),
                "pytest_exit_code": exit_code,
            },
        )
    exit_with_code(0)


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

