"""CLI-команда `bioetl-vocab-audit`."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from requests.exceptions import HTTPError, Timeout

from bioetl.cli.runner import run_app
from bioetl.cli.tools._typer import TyperApp, create_app
from bioetl.core.api_client import CircuitBreakerOpenError
from bioetl.core.errors import BioETLError
from bioetl.tools.vocab_audit import DEFAULT_META, DEFAULT_OUTPUT, VocabAuditResult
from bioetl.tools.vocab_audit import (
    audit_vocabularies as audit_vocabularies_sync,
)

typer = cast(Any, importlib.import_module("typer"))

__all__ = [
    "app",
    "main",
    "run",
    "audit_vocabularies",
    "DEFAULT_OUTPUT",
    "DEFAULT_META",
    "VocabAuditResult",
]

audit_vocabularies = audit_vocabularies_sync


app: TyperApp = create_app(
    name="bioetl-vocab-audit",
    help_text="Проверь словари ChEMBL и сформируй отчёт",
)


@app.command()
def main(
    store: Path | None = typer.Option(
        None,
        "--store",
        help="Каталог или YAML со словарями ChEMBL.",
        exists=True,
        file_okay=True,
        dir_okay=True,
        readable=True,
    ),
    output: Path = typer.Option(
        DEFAULT_OUTPUT,
        "--output",
        help="Путь к CSV-отчёту аудита словарей.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
    meta: Path = typer.Option(
        DEFAULT_META,
        "--meta",
        help="Путь к meta.yaml отчёта.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
    pages: int = typer.Option(
        10,
        "--pages",
        help="Количество страниц Chembl API для выборки значений.",
    ),
    page_size: int = typer.Option(
        1000,
        "--page-size",
        help="Размер страницы Chembl API.",
    ),
) -> None:
    """Запускает аудит словарей."""

    try:
        result = audit_vocabularies(
            store=store.resolve() if store else None,
            output=output,
            meta=meta,
            pages=pages,
            page_size=page_size,
        )
    except (BioETLError, CircuitBreakerOpenError, HTTPError, Timeout) as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    typer.echo(
        "Аудит словарей завершён: "
        f"{len(result.rows)} записей, CSV {result.output.resolve()}, meta {result.meta.resolve()}"
    )
    raise typer.Exit(code=0)


def run() -> None:
    """Точка входа для Typer-приложения."""
    run_app(app)


if __name__ == "__main__":
    run()

