"""CLI для аудита словарей ChEMBL."""

from __future__ import annotations

from pathlib import Path

import typer

from bioetl.cli.tools import create_app
from bioetl.tools.vocab_audit import audit_vocabularies

app = create_app(
    name="bioetl-vocab-audit",
    help_text="Сравнение значений ChEMBL с локальными словарями",
)


@app.command()
def main(
    store: Path | None = typer.Option(None, help="Путь к директории словарей или агрегатному YAML"),
    output: Path = typer.Option(Path("artifacts/vocab_audit.csv"), help="Путь для CSV отчёта"),
    meta: Path = typer.Option(Path("artifacts/vocab_audit.meta.yaml"), help="Путь для meta.yaml"),
    pages: int = typer.Option(10, min=1, help="Количество страниц выборки"),
    page_size: int = typer.Option(1000, min=10, help="Размер страницы при запросе API"),
) -> None:
    """Запустить аудит словарей."""

    result = audit_vocabularies(
        store=store,
        output=output,
        meta=meta,
        pages=pages,
        page_size=page_size,
    )
    typer.echo(
        f"Аудит завершён: {len(result.rows)} строк, отчёт {result.output}, мета {result.meta}"
    )


def run() -> None:
    app()
