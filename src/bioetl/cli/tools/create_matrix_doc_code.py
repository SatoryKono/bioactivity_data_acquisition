"""CLI для генерации матрицы Doc↔Code."""

from __future__ import annotations

from pathlib import Path

import typer

from bioetl.cli.tools import create_app
from bioetl.tools.create_matrix_doc_code import write_matrix

app = create_app(
    name="bioetl-create-matrix-doc-code",
    help_text="Генерация матрицы трассировки документации к коду",
)


@app.command()
def main(
    artifacts: Path = typer.Option(
        Path("artifacts"),
        help="Каталог для записи CSV/JSON",
    ),
) -> None:
    """Сформировать матрицу Doc↔Code."""

    result = write_matrix(artifacts_dir=artifacts.resolve())
    typer.echo(
        f"Матрица с {len(result.rows)} строками записана в "
        f"{result.csv_path} и {result.json_path}"
    )


def run() -> None:
    app()

