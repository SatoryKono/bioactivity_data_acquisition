"""CLI для проверки артефактов в data/output."""

from __future__ import annotations

import typer

from bioetl.cli.tools import create_app, run_app
from bioetl.tools.check_output_artifacts import MAX_BYTES, check_output_artifacts

app = create_app(
    name="bioetl-check-output-artifacts",
    help_text="Проверка артефактов в каталоге data/output",
)


@app.command()
def main(max_bytes: int = typer.Option(MAX_BYTES, help="Порог размера файла в байтах")) -> None:
    """Проверить каталог data/output на наличие проблем."""

    errors = check_output_artifacts(max_bytes=max_bytes)
    if errors:
        for error in errors:
            typer.secho(error, fg=typer.colors.RED)
        raise typer.Exit(code=1)
    typer.echo("Каталог data/output чистый: артефактов не обнаружено")


def run() -> None:
    run_app(app)
