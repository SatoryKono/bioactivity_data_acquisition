"""CLI command ``bioetl-check-comments``."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.tools.check_comments import run_comment_check as run_comment_check_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "run_comment_check"]

run_comment_check = run_comment_check_sync

app: TyperApp = create_app(
    name="bioetl-check-comments",
    help_text="Validate code comments and TODO markers",
)


@app.command()
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


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

