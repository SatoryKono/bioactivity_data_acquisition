"""CLI для каталогизации сигнатур ключевых сущностей."""

from __future__ import annotations

from pathlib import Path

import typer

from bioetl.cli.tools import create_app, runner_factory
from bioetl.tools.catalog_code_symbols import catalog_code_symbols

app = create_app(
    name="bioetl-catalog-code-symbols",
    help_text="Сбор сигнатур PipelineBase, конфигов и CLI-команд",
)


@app.command()
def main(
    artifacts: Path = typer.Option(
        Path("artifacts"),
        help="Каталог для сохранения отчётов",
    ),
) -> None:
    """Собрать каталог кодовых сущностей и записать артефакты."""

    result = catalog_code_symbols(artifacts_dir=artifacts.resolve())
    typer.echo(
        "Сигнатуры сохранены: "
        f"pipeline методы={len(result.pipeline_signatures)}, "
        f"CLI команды={len(result.cli_commands)}"
    )
run = runner_factory(app)
run.__doc__ = "Запуск Typer-приложения."
