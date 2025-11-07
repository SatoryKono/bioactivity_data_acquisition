"""CLI для генерации отчёта pytest/coverage."""

from __future__ import annotations

from pathlib import Path

import typer

from bioetl.cli.tools import create_app
from bioetl.core.test_report_artifacts import TEST_REPORTS_ROOT
from bioetl.tools.run_test_report import generate_test_report

app = create_app(
    name="bioetl-run-test-report",
    help_text="Генерирует отчёты pytest и coverage c meta.yaml",
)


@app.command()
def main(
    output_root: Path = typer.Option(
        TEST_REPORTS_ROOT,
        help="Каталог для сохранения артефактов",
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
) -> None:
    """Запустить pytest и сформировать отчёты."""

    exit_code = generate_test_report(output_root=output_root.resolve())
    raise typer.Exit(code=exit_code)


def run() -> None:
    app()

