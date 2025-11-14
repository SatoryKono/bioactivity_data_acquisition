"""CLI command ``bioetl-semantic-diff``."""

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
from bioetl.tools.semantic_diff import run_semantic_diff as run_semantic_diff_sync

typer: Any = get_typer()

__all__ = ["app", "main", "run", "run_semantic_diff"]

run_semantic_diff = run_semantic_diff_sync


def main() -> None:
    """Run the semantic diff workflow."""

    try:
        report_path = run_semantic_diff()
    except Exception as exc:  # noqa: BLE001
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Semantic diff failed: {exc}",
            context={
                "command": "bioetl-semantic-diff",
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    typer.echo(f"Semantic diff report written to: {report_path.resolve()}")
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-semantic-diff",
    help_text="Compare documentation and code to produce a diff",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

