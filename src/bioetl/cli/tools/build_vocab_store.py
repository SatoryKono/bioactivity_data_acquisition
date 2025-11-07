"""CLI для сборки словаря ChEMBL."""

from __future__ import annotations

from pathlib import Path

import typer

from bioetl.cli.tools import create_app
from bioetl.etl.vocab_store import VocabStoreError
from bioetl.tools.build_vocab_store import build_vocab_store

app = create_app(
    name="bioetl-build-vocab-store",
    help_text="Агрегирует YAML-словарь ChEMBL",
)


@app.command()
def main(
    src: Path = typer.Option(
        Path("configs/dictionaries"),
        help="Каталог с отдельными словарями",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
    output: Path = typer.Option(
        Path("artifacts/chembl_dictionaries.yaml"),
        help="Итоговый YAML-файл",
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
) -> None:
    """Построить агрегированный словарь."""

    try:
        result = build_vocab_store(src=src, output=output)
        typer.echo(f"Aggregated vocab store written to {result}")
    except VocabStoreError as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc


def run() -> None:
    app()

