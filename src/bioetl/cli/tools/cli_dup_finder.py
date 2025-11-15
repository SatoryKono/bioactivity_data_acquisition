"""CLI command ``bioetl-dup-finder``."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from bioetl.cli.cli_entrypoint import TyperApp, get_typer, register_tool_app
from bioetl.cli.tools._logic import cli_dup_finder as cli_dup_finder_impl
from bioetl.core.logging import LogEvents
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_CONFIG, CLI_ERROR_INTERNAL

_LOGIC_EXPORTS = getattr(cli_dup_finder_impl, "__all__", [])
globals().update({symbol: getattr(cli_dup_finder_impl, symbol) for symbol in _LOGIC_EXPORTS})
run_dup_finder_workflow = getattr(cli_dup_finder_impl, "main")
__all__ = [
    * _LOGIC_EXPORTS,
    "run_dup_finder_workflow",
    "app",
    "cli_main",
    "run",
]  # pyright: ignore[reportUnsupportedDunderAll]

typer: Any = get_typer()

DEFAULT_ROOT = cli_dup_finder_impl.DEFAULT_ROOT
DEFAULT_OUT_DIR = cli_dup_finder_impl.DEFAULT_OUTPUT_DIR


def _normalize_out(value: str) -> Path | None:
    normalized = value.strip()
    if not normalized:
        return DEFAULT_OUT_DIR
    if normalized == "-":
        return Path(normalized)
    return Path(normalized)


def cli_main(
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
    except typer.Exit:
        raise
    except ValueError as exc:
        CliCommandBase.emit_error(
            template=CLI_ERROR_CONFIG,
            message=f"Duplicate finder configuration error: {exc}",
            context={
                "command": "bioetl-dup-finder",
                "root": str(root),
                "out": str(out_path),
                "format": fmt,
                "exception_type": exc.__class__.__name__,
            },
            exit_code=1,
            cause=exc,
        )
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Duplicate finder failed: {exc}",
            event=LogEvents.DUP_FINDER_FAILED,
            context={
                "command": "bioetl-dup-finder",
                "root": str(root),
                "out": str(out_path),
                "format": fmt,
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    typer.echo("Duplicate finder completed successfully.")
    CliCommandBase.exit(0)


app: TyperApp
run: Callable[[], None]
app, run = register_tool_app(
    name="bioetl-dup-finder",
    help_text="Detect duplicate and near-duplicate code fragments across the repo.",
    main_fn=cli_main,
)


if __name__ == "__main__":
    run()
