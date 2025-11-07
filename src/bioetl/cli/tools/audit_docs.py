"""CLI для аудита документации."""

from __future__ import annotations

from pathlib import Path

import typer

from bioetl.cli.tools import create_app
from bioetl.tools.audit_docs import run_audit

app = create_app(
    name="bioetl-audit-docs",
    help_text="Проводит аудит документации и генерирует отчёты",
)


@app.command()
def main(
    artifacts: Path = typer.Option(
        Path("artifacts"),
        help="Каталог для генерации отчётов аудита",
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
) -> None:
    """Запуск аудита документации."""

    run_audit(artifacts_dir=artifacts.resolve())
    typer.echo(f"Аудит завершён, отчёты находятся в {artifacts.resolve()}")


def run() -> None:
    """Запускает Typer-приложение."""

    app()

