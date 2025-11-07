"""CLI для запуска проверки ссылок."""

from __future__ import annotations

import typer

from bioetl.cli.tools import create_app
from bioetl.tools.link_check import run_link_check

app = create_app(
    name="bioetl-link-check",
    help_text="Проверка ссылок в документации через lychee",
)


@app.command()
def main(timeout: int = typer.Option(300, help="Таймаут запуска lychee в секундах")) -> None:
    """Выполнить проверку ссылок."""

    exit_code = run_link_check(timeout_seconds=timeout)
    if exit_code != 0:
        typer.secho(f"Lychee завершился с кодом {exit_code}", fg=typer.colors.RED)
        raise typer.Exit(code=exit_code)
    typer.echo("Отчёт по ссылкам сформирован")


def run() -> None:
    app()
