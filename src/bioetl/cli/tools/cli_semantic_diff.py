"""CLI command ``bioetl-semantic-diff``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_semantic_diff as cli_semantic_diff_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL

_LOGIC_EXPORTS = getattr(cli_semantic_diff_impl, "__all__", [])
globals().update({symbol: getattr(cli_semantic_diff_impl, symbol) for symbol in _LOGIC_EXPORTS})
__all__ = [* _LOGIC_EXPORTS, "app", "cli_main", "run"]

typer: Any = get_typer()


def cli_main() -> None:
    """Run the semantic diff workflow."""

    try:
        report_path = cli_semantic_diff_impl.run_semantic_diff()
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Semantic diff failed: {exc}",
            context={
                "command": "bioetl-semantic-diff",
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    typer.echo(f"Semantic diff report written to: {report_path.resolve()}")
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-semantic-diff",
    help_text="Compare documentation and code to produce a diff",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()