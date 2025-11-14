"""CLI command ``bioetl-determinism-check``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.tools import emit_tool_error, exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.core.runtime.cli_errors import CLI_ERROR_CONFIG, CLI_ERROR_INTERNAL
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
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Determinism check failed: {exc}",
            context={
                "command": "bioetl-determinism-check",
                "exception_type": exc.__class__.__name__,
                "pipelines": targets,
            },
            cause=exc,
        )

    if not results:
        emit_tool_error(
            template=CLI_ERROR_CONFIG,
            message="No pipelines found for determinism check",
            context={
                "command": "bioetl-determinism-check",
                "pipelines": targets,
            },
        )

    non_deterministic = [
        name for name, item in results.items() if not item.deterministic
    ]
    first_result = next(iter(results.values()))

    if non_deterministic:
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=(
                "Non-deterministic pipelines detected: "
                f"{', '.join(non_deterministic)}. "
                f"Report: {first_result.report_path.resolve()}"
            ),
            context={
                "command": "bioetl-determinism-check",
                "pipelines": tuple(non_deterministic),
                "report_path": str(first_result.report_path.resolve()),
            },
        )

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

