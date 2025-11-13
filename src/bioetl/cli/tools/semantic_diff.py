"""CLI для семантического diff документации и кода."""

from __future__ import annotations

import typer

from bioetl.cli.tools import create_app, run_app
from bioetl.tools.semantic_diff import run_semantic_diff

app = create_app(
    name="bioetl-semantic-diff",
    help_text="Сравнение сигнатур и флагов между документацией и кодом",
)


@app.command()
def main() -> None:
    """Сформировать отчёт семантического diff."""

    report_path = run_semantic_diff()
    typer.echo(f"Семантический diff записан в {report_path}")


def run() -> None:
    run_app(app)
