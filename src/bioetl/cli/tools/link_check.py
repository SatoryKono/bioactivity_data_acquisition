"""CLI command ``bioetl-link-check``."""

from __future__ import annotations

import importlib
from typing import Any, cast

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.tools.link_check import run_link_check as run_link_check_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "run_link_check"]

run_link_check = run_link_check_sync

app: TyperApp = create_app(
    name="bioetl-link-check",
    help_text="Verify documentation links via lychee",
)


@app.command()
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


def run() -> None:
    """Execute the Typer application."""
    run_app(app)


if __name__ == "__main__":
    run()

