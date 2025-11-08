"""CLI для запуска doctest CLI-примеров."""

from __future__ import annotations

import typer

from bioetl.cli.tools import create_app, runner_factory
from bioetl.tools.doctest_cli import extract_cli_examples, run_examples

app = create_app(
    name="bioetl-doctest-cli",
    help_text="Выполнение CLI-примеров из документации в режиме dry-run",
)


@app.command()
def main() -> None:
    """Запустить все CLI-примеры из документации."""

    examples = extract_cli_examples()
    results, report_path = run_examples(examples)
    failed = [item for item in results if item.exit_code != 0]
    if failed:
        typer.secho(
            f"Не все примеры прошли успешно. См. отчёт {report_path}",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    typer.echo(f"Все {len(results)} примеров прошли успешно. Отчёт {report_path}")
run = runner_factory(app)
