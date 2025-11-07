"""CLI для проверки детерминизма пайплайнов."""

from __future__ import annotations

import typer

from bioetl.cli.tools import create_app
from bioetl.tools.determinism_check import run_determinism_check

app = create_app(
    name="bioetl-determinism-check",
    help_text="Запуск двух прогонов пайплайнов и сравнение логов",
)


@app.command()
def main(
    pipeline: str | None = typer.Option(
        None,
        help="Ограничить проверку конкретным пайплайном (по умолчанию два основных)",
    ),
) -> None:
    """Выполнить проверку детерминизма."""

    pipelines = (pipeline,) if pipeline else None
    results = run_determinism_check(pipelines=pipelines)
    non_deterministic = [name for name, item in results.items() if not item.deterministic]
    if non_deterministic:
        typer.secho(
            "Обнаружены недетерминированные пайплайны: " + ", ".join(non_deterministic),
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    typer.echo("Все проверенные пайплайны детерминированы")


def run() -> None:
    app()
