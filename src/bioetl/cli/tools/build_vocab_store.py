"""CLI command ``bioetl-build-vocab-store``."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.tools.build_vocab_store import build_vocab_store as build_vocab_store_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "build_vocab_store", "main", "run"]
build_vocab_store = build_vocab_store_sync

app: TyperApp = create_app(
    name="bioetl-build-vocab-store",
    help_text="Assemble the aggregated ChEMBL vocabulary and export YAML",
)


@app.command()
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
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    typer.echo(f"Aggregated vocabulary written to {result_path}")
    exit_with_code(0)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

