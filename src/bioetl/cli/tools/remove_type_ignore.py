"""CLI command ``bioetl-remove-type-ignore``."""

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
from bioetl.tools.remove_type_ignore import remove_type_ignore as remove_type_ignore_sync

typer: Any = get_typer()

__all__ = ["app", "main", "run", "remove_type_ignore"]

remove_type_ignore = remove_type_ignore_sync

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


app: TyperApp = create_simple_tool_app(
    name="bioetl-remove-type-ignore",
    help_text="Remove type ignore directives from source files",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

