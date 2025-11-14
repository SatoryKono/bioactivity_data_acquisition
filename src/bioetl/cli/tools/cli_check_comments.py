"""CLI command ``bioetl-check-comments``."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from bioetl.cli.cli_entrypoint import TyperApp, get_typer, register_tool_app
from bioetl.cli.tools._logic import cli_check_comments as cli_check_comments_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_CONFIG, CLI_ERROR_INTERNAL

_LOGIC_EXPORTS = getattr(cli_check_comments_impl, "__all__", [])
globals().update({symbol: getattr(cli_check_comments_impl, symbol) for symbol in _LOGIC_EXPORTS})
run_comment_check = getattr(cli_check_comments_impl, "run_comment_check")
__all__ = [* _LOGIC_EXPORTS, "run_comment_check", "app", "cli_main", "run"]

typer: Any = get_typer()


def cli_main(
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
    except typer.Exit:
        raise
    except NotImplementedError as exc:
        CliCommandBase.emit_error(
            template=CLI_ERROR_CONFIG,
            message=f"Comment check configuration error: {exc}",
            context={
                "command": "bioetl-check-comments",
                "root": str(resolved_root) if resolved_root else None,
                "exception_type": exc.__class__.__name__,
            },
        )
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Comment check failed: {exc}",
            context={
                "command": "bioetl-check-comments",
                "root": str(resolved_root) if resolved_root else None,
                "exception_type": exc.__class__.__name__,
            },
        )

    typer.echo("Comment check completed without errors")
    CliCommandBase.exit(0)


app: TyperApp
run: Callable[[], None]
app, run = register_tool_app(
    name="bioetl-check-comments",
    help_text="Validate code comments and TODO markers",
    main_fn=cli_main,
)


if __name__ == "__main__":
    run()
