"""CLI command ``bioetl-remove-type-ignore``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_remove_type_ignore as cli_remove_type_ignore_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL

TYPE_IGNORE_PATTERN = cli_remove_type_ignore_impl.TYPE_IGNORE_PATTERN
_iter_python_files = cli_remove_type_ignore_impl._iter_python_files
_cleanse_file = cli_remove_type_ignore_impl._cleanse_file
remove_type_ignore = cli_remove_type_ignore_impl.remove_type_ignore

__all__ = (
    "TYPE_IGNORE_PATTERN",
    "_iter_python_files",
    "_cleanse_file",
    "remove_type_ignore",
    "app",
    "cli_main",
    "run",
)

typer: Any = get_typer()


def cli_main(
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
    removed: int
    try:
        removed = remove_type_ignore(root=resolved_root)
    except typer.Exit:
        raise
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
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
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-remove-type-ignore",
    help_text="Remove type ignore directives from source files",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()