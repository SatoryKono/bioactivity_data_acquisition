"""CLI command ``bioetl-dup-finder``."""

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
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL
from bioetl.tools import get_project_root
from bioetl.tools.dup_finder import main as run_dup_finder_workflow

typer: Any = get_typer()

DEFAULT_ROOT = get_project_root()
DEFAULT_OUT_DIR = DEFAULT_ROOT / "artifacts" / "dup_finder"

__all__ = ["app", "main", "run"]


def _normalize_out(value: str) -> Path | None:
    normalized = value.strip()
    if not normalized:
        return DEFAULT_OUT_DIR
    if normalized == "-":
        return Path(normalized)
    return Path(normalized)


def main(
    root: Path = typer.Option(
        DEFAULT_ROOT,
        "--root",
        help="Repository root to scan for duplicate code fragments.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
        show_default=True,
    ),
    out: str = typer.Option(
        str(DEFAULT_OUT_DIR),
        "--out",
        help="Directory for artifacts or '-' to stream reports to STDOUT.",
        show_default=True,
    ),
    fmt: str = typer.Option(
        "md,csv",
        "--format",
        help="Comma-separated list of output formats (md,csv).",
        show_default=True,
    ),
) -> None:
    """Execute the duplicate finder tool with Typer-based argument parsing."""

    out_path = _normalize_out(out)
    try:
        run_dup_finder_workflow(root=root, out=out_path, fmt=fmt)
    except ValueError as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.YELLOW)
        exit_with_code(1, cause=exc)
    except Exception as exc:  # noqa: BLE001
        log = UnifiedLogger.get(__name__)
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Duplicate finder failed: {exc}",
            logger=log,
            event=LogEvents.DUP_FINDER_FAILED,
            context={"exception_type": type(exc).__name__, "exc_info": True},
        )
        typer.secho("Duplicate finder failed, see logs for details.", err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    typer.echo("Duplicate finder completed successfully.")
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-dup-finder",
    help_text="Detect duplicate and near-duplicate code fragments across the repo.",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

