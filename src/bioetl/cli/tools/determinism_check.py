"""CLI-команда `bioetl-determinism-check`."""

from __future__ import annotations

import importlib
from typing import Any, cast

from bioetl.cli.tools._typer import TyperApp, create_app
from bioetl.tools.determinism_check import DeterminismRunResult
from bioetl.tools.determinism_check import (
    run_determinism_check as run_determinism_check_sync,
)

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "run_determinism_check", "DeterminismRunResult"]

run_determinism_check = run_determinism_check_sync

app: TyperApp = create_app(
    name="bioetl-determinism-check",
    help_text="Запусти два прогона и сравни логи",
)


@app.command()
def main(
    pipeline: list[str] | None = typer.Option(
        None,
        "--pipeline",
        "-p",
        help="Пайплайн для проверки (можно указать несколько флагов). По умолчанию activity_chembl и assay_chembl.",
    ),
) -> None:
    """Выполняет проверку детерминизма указанных пайплайнов."""

    targets = tuple(pipeline) if pipeline else None

    try:
        results = run_determinism_check(pipelines=targets)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    if not results:
        typer.secho("Не найдены пайплайны для проверки", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1)

    non_deterministic = [
        name for name, item in results.items() if not item.deterministic
    ]
    first_result = next(iter(results.values()))

    if non_deterministic:
        typer.secho(
            "Обнаружены недетерминированные пайплайны: " + ", ".join(non_deterministic),
            err=True,
            fg=typer.colors.RED,
        )
        typer.echo(f"Подробности см. в отчёте: {first_result.report_path.resolve()}")
        raise typer.Exit(code=1)

    typer.echo(
        "Все проверенные пайплайны детерминированы. "
        f"Отчёт: {first_result.report_path.resolve()}"
    )
    raise typer.Exit(code=0)


def run() -> None:
    """Запускает Typer-приложение."""

    app()


if __name__ == "__main__":
    run()

