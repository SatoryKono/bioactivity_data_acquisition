"""CLI для удаления директив type: ignore."""

from __future__ import annotations

from pathlib import Path

import typer

from bioetl.cli.tools import create_app, run_app
from bioetl.tools.remove_type_ignore import remove_type_ignore

app = create_app(
    name="bioetl-remove-type-ignore",
    help_text="Удаляет директивы type: ignore из исходников",
)


@app.command()
def main(
    root: Path = typer.Option(Path("."), help="Корневой каталог для обработки"),
) -> None:
    """Удалить директивы type: ignore."""

    count = remove_type_ignore(root=root.resolve())
    typer.echo(f"Удалено {count} директив type: ignore")


def run() -> None:
    run_app(app)
