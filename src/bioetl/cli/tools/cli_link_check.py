"""CLI command ``bioetl-link-check``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_link_check as cli_link_check_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL

_LOGIC_EXPORTS = getattr(cli_link_check_impl, "__all__", [])
globals().update({symbol: getattr(cli_link_check_impl, symbol) for symbol in _LOGIC_EXPORTS})
__all__ = [* _LOGIC_EXPORTS, "app", "cli_main", "run"]

typer: Any = get_typer()


def cli_main(
    timeout_seconds: int = typer.Option(
        300,
        "--timeout",
        help="Execution timeout for lychee in seconds.",
    ),
) -> None:
    """Run the documentation link validation."""

    try:
        exit_code = cli_link_check_impl.run_link_check(timeout_seconds=timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
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
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Link check failed with exit code {exit_code}",
            context={
                "command": "bioetl-link-check",
                "timeout_seconds": timeout_seconds,
                "lychee_exit_code": exit_code,
            },
        )
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-link-check",
    help_text="Verify documentation links via lychee",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""
    run_app(app)


if __name__ == "__main__":
    run()