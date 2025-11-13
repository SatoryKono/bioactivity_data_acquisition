"""CLI command ``bioetl-remove-type-ignore``."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.tools.remove_type_ignore import remove_type_ignore as remove_type_ignore_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "remove_type_ignore"]

remove_type_ignore = remove_type_ignore_sync

app: TyperApp = create_app(
    name="bioetl-remove-type-ignore",
    help_text="Remove type ignore directives from source files",
)


@app.command()
def main(
    root: Path | None = typer.Option(
        None,
        "--root",
        help="Project directory to process (defaults to the repository root).",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
    ),
) -> None:
    """Remove ``type: ignore`` directives from the selected tree."""

    try:
        resolved_root = root.resolve() if root is not None else None
        removed = remove_type_ignore(root=resolved_root)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    typer.echo(f"Removed type ignore directives: {removed}")
    exit_with_code(0)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

