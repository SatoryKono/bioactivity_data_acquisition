"""CLI-команда `bioetl-doctest-cli`."""

from __future__ import annotations

import importlib
from typing import Any, cast

from tools import TyperApp, create_app
from bioetl.tools.doctest_cli import (
    CLIExample,
    CLIExampleResult,
    extract_cli_examples,
)
from bioetl.tools.doctest_cli import (
    run_examples as run_examples_sync,
)

typer = cast(Any, importlib.import_module("typer"))

__all__ = [
    "app",
    "main",
    "run",
    "run_examples",
    "extract_cli_examples",
    "CLIExample",
    "CLIExampleResult",
]

run_examples = run_examples_sync


app: TyperApp = create_app(
    name="bioetl-doctest-cli",
    help_text="Запусти CLI-примеры и сформируй отчёт",
)


@app.command()
def main() -> None:
    """Запускает CLI-doctest и анализирует результаты."""

    try:
        results, report_path = run_examples()
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    failed = [item for item in results if item.exit_code != 0]
    if failed:
        typer.secho(
            f"Не все CLI-примеры прошли успешно ({len(failed)} из {len(results)}).",
            err=True,
            fg=typer.colors.RED,
        )
        typer.echo(f"Отчёт доступен по пути: {report_path.resolve()}")
        raise typer.Exit(code=1)

    typer.echo(
        f"Все {len(results)} CLI-примеров выполнены успешно. "
        f"Отчёт: {report_path.resolve()}"
    )
    raise typer.Exit(code=0)


def run() -> None:
    """Запускает Typer-приложение."""

    app()
