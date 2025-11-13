"""CLI command ``bioetl-check-comments``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
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

    try:
        run_comment_check(root=root.resolve() if root else None)
    except NotImplementedError as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.YELLOW)
        exit_with_code(1, cause=exc)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

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

