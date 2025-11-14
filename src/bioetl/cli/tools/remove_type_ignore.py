"""CLI command ``bioetl-remove-type-ignore``."""

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
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL
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

    resolved_root = root.resolve() if root is not None else None
    try:
        removed = remove_type_ignore(root=resolved_root)
    except Exception as exc:  # noqa: BLE001
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Failed to remove type ignore directives: {exc}",
            context={
                "command": "bioetl-remove-type-ignore",
                "root": str(resolved_root) if resolved_root else None,
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

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

