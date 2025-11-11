"""CLI-команда `bioetl-schema-guard`."""

from __future__ import annotations

import importlib
from typing import Any, cast

from bioetl.cli.runner import run_app
from bioetl.cli.tools._typer import TyperApp, create_app
from bioetl.tools.schema_guard import run_schema_guard as run_schema_guard_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "run_schema_guard"]
run_schema_guard = run_schema_guard_sync

app: TyperApp = create_app(
    name="bioetl-schema-guard",
    help_text="Проверь конфиги пайплайнов и Pandera реестр",
)


@app.command()
def main() -> None:
    """Запускает проверку конфигураций и схем."""

    try:
        results, registry_errors, report_path = run_schema_guard()
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    invalid_configs = [
        name for name, payload in results.items() if not payload.get("valid", False)
    ]

    if invalid_configs or registry_errors:
        typer.secho("Найдены проблемы в конфигурациях или реестре схем:", fg=typer.colors.RED)
        if invalid_configs:
            typer.echo(" - Некорректные конфигурации: " + ", ".join(sorted(invalid_configs)))
        if registry_errors:
            typer.echo(" - Ошибки реестра схем:")
            for error in registry_errors:
                typer.echo(f"   * {error}")
        typer.echo(f"Отчёт: {report_path.resolve()}")
        raise typer.Exit(code=1)

    typer.echo(f"Все конфигурации валидны. Отчёт: {report_path.resolve()}")
    raise typer.Exit(code=0)


def run() -> None:
    """Точка входа для Typer-приложения."""
    run_app(app)


if __name__ == "__main__":
    run()
