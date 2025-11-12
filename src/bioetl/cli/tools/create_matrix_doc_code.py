"""CLI-команда `bioetl-create-matrix-doc-code`."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.clients.exceptions import HTTPError, Timeout
from bioetl.core.api_client import CircuitBreakerOpenError
from bioetl.core.errors import BioETLError
from bioetl.tools.create_matrix_doc_code import DocCodeMatrix, build_matrix
from bioetl.tools.create_matrix_doc_code import (
    write_matrix as write_matrix_sync,
)

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "write_matrix", "build_matrix", "DocCodeMatrix"]

write_matrix = write_matrix_sync

app: TyperApp = create_app(
    name="bioetl-create-matrix-doc-code",
    help_text="Сгенерируй матрицу Doc↔Code и экспортируй артефакты",
)


@app.command()
def main(
    artifacts: Path = typer.Option(
        Path("artifacts"),
        "--artifacts",
        help="Каталог для сохранения матрицы Doc↔Code.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
) -> None:
    """Формирует матрицу соответствия документации и кода."""

    try:
        result = write_matrix(artifacts_dir=artifacts.resolve())
    except (BioETLError, CircuitBreakerOpenError, HTTPError, Timeout) as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    typer.echo(
        f"Матрица с {len(result.rows)} строками записана в "
        f"{result.csv_path.resolve()} и {result.json_path.resolve()}"
    )
    raise typer.Exit(code=0)


def run() -> None:
    """Запускает Typer-приложение."""

    run_app(app)


if __name__ == "__main__":
    run()

