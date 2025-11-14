"""CLI command ``bioetl-qc-boundary-check`` for static QC import boundary checks."""

from __future__ import annotations

from typing import Any

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools.qc_boundary import collect_cli_qc_boundary_report
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)

typer: Any = get_typer()

__all__ = ["app", "main", "run"]

def main() -> None:
    """Run the static import analysis for the CLI↔QC boundary."""

    report = collect_cli_qc_boundary_report()
    if not report.has_violations:
        typer.echo("CLI↔QC boundary is respected, no violations found.")
        exit_with_code(0)

    typer.secho(
        "CLI↔QC boundary violations detected:",
        err=True,
        fg=typer.colors.RED,
    )
    for violation in report.violations:
        typer.secho(
            f"- {violation.source_path}: {violation.format_chain()}",
            err=True,
            fg=typer.colors.RED,
        )
    exit_with_code(1)


app: TyperApp = create_simple_tool_app(
    name="bioetl-qc-boundary-check",
    help_text="Ensure bioetl.cli modules do not import bioetl.qc directly or via re-export.",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()


