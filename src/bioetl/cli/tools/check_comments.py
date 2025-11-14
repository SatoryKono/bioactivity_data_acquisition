"""CLI command ``bioetl-check-comments``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.tools import emit_tool_error, exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.core.runtime.cli_errors import CLI_ERROR_CONFIG, CLI_ERROR_INTERNAL
from bioetl.tools.check_comments import run_comment_check as run_comment_check_sync

typer: Any = get_typer()

__all__ = ["app", "main", "run", "run_comment_check"]

run_comment_check = run_comment_check_sync

def main(
    root: Path | None = typer.Option(
        None,
        "--root",
        help="Project directory to inspect (defaults to the repository root).",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
) -> None:
    """Run the comment quality check."""

    resolved_root = root.resolve() if root else None
    try:
        run_comment_check(root=resolved_root)
    except NotImplementedError as exc:
        emit_tool_error(
            template=CLI_ERROR_CONFIG,
            message=f"Comment check configuration error: {exc}",
            context={
                "command": "bioetl-check-comments",
                "root": str(resolved_root) if resolved_root else None,
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )
    except Exception as exc:  # noqa: BLE001
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Comment check failed: {exc}",
            context={
                "command": "bioetl-check-comments",
                "root": str(resolved_root) if resolved_root else None,
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    typer.echo("Comment check completed without errors")
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-check-comments",
    help_text="Validate code comments and TODO markers",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

