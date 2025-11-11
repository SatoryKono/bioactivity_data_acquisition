"""CLI-команда `bioetl-catalog-code-symbols`."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from requests.exceptions import HTTPError, Timeout

from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.core.api_client import CircuitBreakerOpenError
from bioetl.core.errors import BioETLError
from bioetl.tools.catalog_code_symbols import CodeCatalog
from bioetl.tools.catalog_code_symbols import (
    catalog_code_symbols as catalog_code_symbols_sync,
)

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "catalog_code_symbols", "CodeCatalog"]

catalog_code_symbols = catalog_code_symbols_sync

app: TyperApp = create_app(
    name="bioetl-catalog-code-symbols",
    help_text="Собери каталог кодовых сущностей и отчётов",
)


@app.command()
def main(
    artifacts: Path | None = typer.Option(
        None,
        "--artifacts",
        help="Каталог, куда будут записаны артефакты каталога.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
) -> None:
    """Запускает сбор каталога кодовых сущностей."""

    try:
        result = catalog_code_symbols(artifacts_dir=artifacts.resolve() if artifacts else None)
    except (BioETLError, CircuitBreakerOpenError, HTTPError, Timeout) as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    typer.echo(
        f"Каталог обновлён: {result.json_path.resolve()} и {result.cli_path.resolve()}"
    )
    raise typer.Exit(code=0)


def run() -> None:
    """Запускает Typer-приложение."""

    run_app(app)


if __name__ == "__main__":
    run()

