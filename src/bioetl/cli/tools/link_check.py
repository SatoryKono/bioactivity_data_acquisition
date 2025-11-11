"""CLI-команда `bioetl-link-check`."""

from __future__ import annotations

import importlib
from typing import Any, cast

from bioetl.cli.runner import run_app
from bioetl.cli.tools._typer import TyperApp, create_app
from bioetl.tools.link_check import run_link_check as run_link_check_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "run_link_check"]

run_link_check = run_link_check_sync

app: TyperApp = create_app(
    name="bioetl-link-check",
    help_text="Проверь документационные ссылки через lychee",
)


@app.command()
def main(
    timeout_seconds: int = typer.Option(
        300,
        "--timeout",
        help="Таймаут выполнения lychee в секундах.",
    ),
) -> None:
    """Выполняет проверку ссылок."""

    try:
        exit_code = run_link_check(timeout_seconds=timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    if exit_code == 0:
        typer.echo("Проверка ссылок завершена успешно")
    else:
        typer.secho(
            f"Проверка ссылок завершилась с ошибками (exit={exit_code})",
            err=True,
            fg=typer.colors.RED,
        )
    raise typer.Exit(code=exit_code)


def run() -> None:
    """Точка входа для Typer-приложения."""
    run_app(app)


if __name__ == "__main__":
    run()

