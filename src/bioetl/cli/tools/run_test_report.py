"""CLI-команда `bioetl-run-test-report`."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from bioetl.cli.tools._typer import TyperApp, create_app
from bioetl.tools.run_test_report import TEST_REPORTS_ROOT
from bioetl.tools.run_test_report import (
    generate_test_report as generate_test_report_sync,
)

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "generate_test_report", "TEST_REPORTS_ROOT"]

generate_test_report = generate_test_report_sync

app: TyperApp = create_app(
    name="bioetl-run-test-report",
    help_text="Сгенерируй pytest и coverage отчёты с метаданными",
)


@app.command()
def main(
    output_root: Path = typer.Option(
        TEST_REPORTS_ROOT,
        "--output-root",
        help="Каталог, куда будут записаны артефакты pytest/coverage.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
) -> None:
    """Запускает pytest и формирует отчёт."""

    try:
        exit_code = generate_test_report(output_root=output_root.resolve())
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    if exit_code == 0:
        typer.echo("Тестовый отчёт сформирован успешно")
    else:
        typer.secho(
            f"pytest завершился с кодом {exit_code}",
            err=True,
            fg=typer.colors.RED,
        )
    raise typer.Exit(code=exit_code)


def run() -> None:
    """Запускает Typer-приложение."""

    app()


if __name__ == "__main__":
    run()

