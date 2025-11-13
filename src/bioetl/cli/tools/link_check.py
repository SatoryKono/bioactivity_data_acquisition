"""CLI command ``bioetl-link-check``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.tools.link_check import run_link_check as run_link_check_sync

typer: Any = get_typer()

__all__ = ["app", "main", "run", "run_link_check"]

run_link_check = run_link_check_sync

def main(
    timeout_seconds: int = typer.Option(
        300,
        "--timeout",
        help="Execution timeout for lychee in seconds.",
    ),
) -> None:
    """Run the documentation link validation."""

    try:
        exit_code = run_link_check(timeout_seconds=timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    if exit_code == 0:
        typer.echo("Link check completed successfully")
    else:
        typer.secho(
            f"Link check failed with errors (exit={exit_code})",
            err=True,
            fg=typer.colors.RED,
        )
    exit_with_code(exit_code)


app: TyperApp = create_simple_tool_app(
    name="bioetl-link-check",
    help_text="Verify documentation links via lychee",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""
    run_app(app)


if __name__ == "__main__":
    run()

