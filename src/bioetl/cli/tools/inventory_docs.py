"""CLI command ``bioetl-inventory-docs``."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.tools.inventory_docs import InventoryResult, collect_markdown_files
from bioetl.tools.inventory_docs import (
    write_inventory as write_inventory_sync,
)

typer = cast(Any, importlib.import_module("typer"))

__all__ = [
    "app",
    "main",
    "run",
    "write_inventory",
    "collect_markdown_files",
    "InventoryResult",
]

write_inventory = write_inventory_sync

app: TyperApp = create_app(
    name="bioetl-inventory-docs",
    help_text="Collect a Markdown document inventory and compute hashes",
)


@app.command()
def main(
    inventory_path: Path = typer.Option(
        Path("artifacts/docs_inventory.txt"),
        "--inventory",
        help="Destination for the Markdown document inventory (text file).",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
    hashes_path: Path = typer.Option(
        Path("artifacts/docs_hashes.txt"),
        "--hashes",
        help="Destination for the Markdown document SHA256 hashes.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
) -> None:
    """Run the documentation inventory routine."""

    try:
        result = write_inventory(
            inventory_path=inventory_path.resolve(),
            hashes_path=hashes_path.resolve(),
        )
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    typer.echo(
        f"Inventory completed: {len(result.files)} files, "
        f"inventory {result.inventory_path.resolve()}, "
        f"hashes {result.hashes_path.resolve()}"
    )
    exit_with_code(0)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

