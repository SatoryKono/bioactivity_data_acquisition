"""CLI-команда `bioetl-check-output-artifacts`."""

from __future__ import annotations

import importlib
from typing import Any, cast

from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.tools.check_output_artifacts import MAX_BYTES
from bioetl.tools.check_output_artifacts import (
    check_output_artifacts as check_output_artifacts_sync,
)

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "check_output_artifacts", "MAX_BYTES"]

check_output_artifacts = check_output_artifacts_sync

app: TyperApp = create_app(
    name="bioetl-check-output-artifacts",
    help_text="Проверь каталог data/output и найди артефакты",
)


@app.command()
def main(
    max_bytes: int = typer.Option(
        MAX_BYTES,
        "--max-bytes",
        help="Порог размера файла (байты), после которого файл считается крупным.",
    ),
) -> None:
    """Запускает проверку каталога артефактов."""

    try:
        errors = check_output_artifacts(max_bytes=max_bytes)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    if errors:
        for message in errors:
            typer.echo(message)
        raise typer.Exit(code=1)

    typer.echo("Каталог data/output чистый")
    raise typer.Exit(code=0)


def run() -> None:
    """Запускает Typer-приложение."""

    run_app(app)


if __name__ == "__main__":
    run()

