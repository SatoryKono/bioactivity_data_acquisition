"""CLI-команда `bioetl-audit-docs`."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from bioetl.tools.audit_docs import run_audit
from tools import TyperApp, create_app

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "run_audit"]

app: TyperApp = create_app(
    name="bioetl-audit-docs",
    help_text="Проведи аудит документации и собери отчёты",
)


@app.command()
def main(
    artifacts: Path = typer.Option(
        Path("artifacts"),
        "--artifacts",
        help="Каталог, куда будут записаны отчёты аудита.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
    )
) -> None:
    """Запускает аудит документации."""

    artifacts_path = artifacts.resolve()
    run_audit(artifacts_dir=artifacts_path)
    typer.echo(f"Аудит завершён, отчёты находятся в {artifacts_path}")
    raise typer.Exit(code=0)


def run() -> None:
    """Выполняет Typer-приложение."""

    app()

