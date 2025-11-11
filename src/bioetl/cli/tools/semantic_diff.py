"""CLI-команда `bioetl-semantic-diff`."""

from __future__ import annotations

import importlib
from typing import Any, cast

from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.tools.semantic_diff import run_semantic_diff as run_semantic_diff_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "run_semantic_diff"]

run_semantic_diff = run_semantic_diff_sync


app: TyperApp = create_app(
    name="bioetl-semantic-diff",
    help_text="Сравни документацию и код и сформируй diff",
)


@app.command()
def main() -> None:
    """Запускает семантический diff."""

    try:
        report_path = run_semantic_diff()
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Отчёт семантического diff записан: {report_path.resolve()}")
    raise typer.Exit(code=0)


def run() -> None:
    """Запускает Typer-приложение."""

    run_app(app)


if __name__ == "__main__":
    run()

