"""CLI для проверки конфигураций и схем."""

from __future__ import annotations

import typer

from bioetl.cli.tools import create_app, runner_factory
from bioetl.tools.schema_guard import run_schema_guard

app = create_app(
    name="bioetl-schema-guard",
    help_text="Валидация конфигураций пайплайнов и реестра схем",
)


@app.command()
def main() -> None:
    """Выполнить проверку схем."""

    results, registry_errors, report_path = run_schema_guard()
    invalid = [name for name, item in results.items() if not item["valid"]]
    if invalid or registry_errors:
        typer.secho(
            f"Найдены проблемы в конфигурациях/схемах. Отчёт: {report_path}",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    typer.echo(f"Все конфигурации валидны. Отчёт: {report_path}")
run = runner_factory(app)
