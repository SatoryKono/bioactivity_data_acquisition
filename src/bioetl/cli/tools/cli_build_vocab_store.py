"""CLI command ``bioetl-build-vocab-store``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_build_vocab_store as cli_build_vocab_store_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL

_LOGIC_EXPORTS = getattr(cli_build_vocab_store_impl, "__all__", [])
globals().update({symbol: getattr(cli_build_vocab_store_impl, symbol) for symbol in _LOGIC_EXPORTS})
build_vocab_store = getattr(cli_build_vocab_store_impl, "build_vocab_store")
__all__ = [* _LOGIC_EXPORTS, "build_vocab_store", "app", "cli_main", "run"]

typer: Any = get_typer()


def cli_main(
    src: Path = typer.Option(
        ...,
        "--src",
        help="Directory containing source vocabularies (YAML).",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
    output: Path = typer.Option(
        ...,
        "--output",
        help="Path to the output YAML file for the aggregated vocabulary.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
) -> None:
    """Aggregate vocabulary files and write the combined YAML."""

    src_path = src.resolve()
    output_path = output.resolve()
    try:
        result_path = build_vocab_store(src=src_path, output=output_path)
    except typer.Exit:
        raise
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Vocabulary store build failed: {exc}",
            context={
                "command": "bioetl-build-vocab-store",
                "src": str(src_path),
                "output": str(output_path),
                "exception_type": exc.__class__.__name__,
            },
        )

    typer.echo(f"Aggregated vocabulary written to {result_path}")
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-build-vocab-store",
    help_text="Assemble the aggregated ChEMBL vocabulary and export YAML",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()