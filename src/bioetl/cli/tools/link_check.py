"""CLI command ``bioetl-link-check``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.tools import emit_tool_error, exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL
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
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Link check execution failed: {exc}",
            context={
                "command": "bioetl-link-check",
                "timeout_seconds": timeout_seconds,
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    if exit_code == 0:
        typer.echo("Link check completed successfully")
    else:
        typer.secho(
            f"Link check failed with errors (exit={exit_code})",
            err=True,
            fg=typer.colors.RED,
        )
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Link check failed with exit code {exit_code}",
            context={
                "command": "bioetl-link-check",
                "timeout_seconds": timeout_seconds,
                "lychee_exit_code": exit_code,
            },
        )
    exit_with_code(0)


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

