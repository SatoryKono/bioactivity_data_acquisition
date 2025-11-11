"""CLI-команда `bioetl-inventory-docs`."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from bioetl.cli.tools._typer import TyperApp, create_app
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
    help_text="Собери инвентарь markdown-доков и посчитай хеши",
)


@app.command()
def main(
    inventory_path: Path = typer.Option(
        Path("artifacts/docs_inventory.txt"),
        "--inventory",
        help="Путь для текстового инвентаря Markdown-документов.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
    hashes_path: Path = typer.Option(
        Path("artifacts/docs_hashes.txt"),
        "--hashes",
        help="Путь для файла с SHA256-хешами Markdown-документов.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
) -> None:
    """Запускает инвентаризацию документации."""

    try:
        result = write_inventory(
            inventory_path=inventory_path.resolve(),
            hashes_path=hashes_path.resolve(),
        )
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    typer.echo(
        f"Инвентарь сформирован: {len(result.files)} файлов, "
        f"инвентарь {result.inventory_path.resolve()}, "
        f"хеши {result.hashes_path.resolve()}"
    )
    raise typer.Exit(code=0)


def run() -> None:
    """Запускает Typer-приложение."""

    app()


if __name__ == "__main__":
    run()

