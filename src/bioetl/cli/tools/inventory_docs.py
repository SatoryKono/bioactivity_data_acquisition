"""CLI-интерфейс для инвентаризации документации."""

from __future__ import annotations

from pathlib import Path

import typer

from bioetl.cli.tools import create_app
from bioetl.tools.inventory_docs import collect_markdown_files, write_inventory

app = create_app(
    name="bioetl-inventory-docs",
    help_text="Инвентаризация markdown-файлов и расчёт хешей",
)


@app.command()
def main(
    inventory: Path = typer.Option(
        Path("artifacts/docs_inventory.txt"),
        help="Путь к итоговому списку файлов",
    ),
    hashes: Path = typer.Option(
        Path("artifacts/docs_hashes.txt"),
        help="Путь к файлу с SHA256 хешами",
    ),
    docs_root: Path | None = typer.Option(
        None,
        help="Необязательный корень документации (по умолчанию docs/)",
    ),
) -> None:
    """Собрать список markdown-файлов и записать их хеши."""

    files = collect_markdown_files(docs_root=docs_root)
    result = write_inventory(inventory_path=inventory, hashes_path=hashes, files=files)
    typer.echo(
        f"Инвентаризация завершена: {len(result.files)} файлов. "
        f"Список -> {result.inventory_path}, хеши -> {result.hashes_path}"
    )


def run() -> None:
    """Запуск Typer-приложения."""

    app()

