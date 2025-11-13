"""CLI command ``bioetl-determinism-check``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.tools.determinism_check import DeterminismRunResult
from bioetl.tools.determinism_check import (
    run_determinism_check as run_determinism_check_sync,
)

typer: Any = get_typer()

__all__ = ["app", "main", "run", "run_determinism_check", "DeterminismRunResult"]

run_determinism_check = run_determinism_check_sync

def main(
    pipeline: list[str] | None = typer.Option(
        None,
        "--pipeline",
        "-p",
        help="Pipeline to verify (can be repeated). Defaults to activity_chembl and assay_chembl.",
    ),
) -> None:
    """Run determinism checks for the selected pipelines."""

    targets = tuple(pipeline) if pipeline else None

    try:
        results = run_determinism_check(pipelines=targets)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    if not results:
        typer.secho("No pipelines found for determinism check", err=True, fg=typer.colors.RED)
        exit_with_code(1)

    non_deterministic = [
        name for name, item in results.items() if not item.deterministic
    ]
    first_result = next(iter(results.values()))

    if non_deterministic:
        typer.secho(
            "Non-deterministic pipelines detected: " + ", ".join(non_deterministic),
            err=True,
            fg=typer.colors.RED,
        )
        typer.echo(f"See report for details: {first_result.report_path.resolve()}")
        exit_with_code(1)

    typer.echo(
        "All inspected pipelines are deterministic. "
        f"Report: {first_result.report_path.resolve()}"
    )
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-determinism-check",
    help_text="Execute two runs and compare their logs",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

