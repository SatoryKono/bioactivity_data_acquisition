"""CLI command ``bioetl-qc-boundary-check`` for static QC import boundary checks."""

from __future__ import annotations

from typing import Any, Callable

from bioetl.devtools.typer_helpers import TyperApp, get_typer, register_tool_app
from bioetl.devtools.cli_qc_boundary import collect_cli_qc_boundary_report
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_CONFIG

typer: Any = get_typer()

__all__ = ["app", "cli_main", "main", "run"]


def cli_main() -> None:
    """Run the static import analysis for the CLI↔QC boundary."""

    report = collect_cli_qc_boundary_report()
    if not report.has_violations:
        typer.echo("CLI↔QC boundary is respected, no violations found.")
        CliCommandBase.exit(0)

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
    CliCommandBase.emit_error(
        template=CLI_ERROR_CONFIG,
        message=f"{len(report.violations)} CLI↔QC boundary violations detected",
        context={
            "command": "bioetl-qc-boundary-check",
            "violation_count": len(report.violations),
        },
    )


def main() -> None:
    """Backward compatible wrapper for legacy entrypoints."""

    cli_main()


app: TyperApp
run: Callable[[], None]
app, run = register_tool_app(
    name="bioetl-qc-boundary-check",
    help_text="Ensure bioetl.cli modules do not import bioetl.qc directly or via re-export.",
    main_fn=cli_main,
)


if __name__ == "__main__":
    run()
