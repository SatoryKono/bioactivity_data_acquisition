"""CLI-команда `bioetl-build-vocab-store`."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.tools.build_vocab_store import build_vocab_store as build_vocab_store_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "build_vocab_store", "main", "run"]
build_vocab_store = build_vocab_store_sync

app: TyperApp = create_app(
    name="bioetl-build-vocab-store",
    help_text="Собери агрегированный словарь ChEMBL и YAML",
)


@app.command()
def main(
    src: Path = typer.Option(
        ...,
        "--src",
        help="Каталог с исходными словарями (YAML).",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
    output: Path = typer.Option(
        ...,
        "--output",
        help="Путь к целевому YAML-файлу агрегированного словаря.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
) -> None:
    """Собирает словари и записывает агрегированный YAML."""

    try:
        result_path = build_vocab_store(src=src, output=output)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Агрегированный словарь записан в {result_path}")
    raise typer.Exit(code=0)


def run() -> None:
    """Запускает Typer-приложение."""

    run_app(app)


if __name__ == "__main__":
    run()

