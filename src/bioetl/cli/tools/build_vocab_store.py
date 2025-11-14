"""CLI command ``bioetl-build-vocab-store``."""

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
from bioetl.tools.build_vocab_store import build_vocab_store as build_vocab_store_sync

typer: Any = get_typer()

__all__ = ["app", "build_vocab_store", "main", "run"]
build_vocab_store = build_vocab_store_sync

def main(
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

    try:
        result_path = build_vocab_store(src=src, output=output)
    except Exception as exc:  # noqa: BLE001
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Vocabulary store build failed: {exc}",
            context={
                "command": "bioetl-build-vocab-store",
                "src": str(src),
                "output": str(output),
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    typer.echo(f"Aggregated vocabulary written to {result_path}")
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-build-vocab-store",
    help_text="Assemble the aggregated ChEMBL vocabulary and export YAML",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

