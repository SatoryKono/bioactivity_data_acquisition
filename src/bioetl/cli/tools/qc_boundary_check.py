"""CLI-команда `bioetl-qc-boundary-check` для статической проверки импорта QC из CLI."""

from __future__ import annotations

import importlib
from typing import Any, cast

from bioetl.cli.tools._qc_boundary import collect_qc_boundary_violations
from bioetl.cli.tools._typer import TyperApp, create_app, run_app

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run"]

app: TyperApp = create_app(
    name="bioetl-qc-boundary-check",
    help_text="Проверяет, что модули bioetl.cli не импортируют bioetl.qc напрямую или через реэкспорт.",
)


@app.command()
def main() -> None:
    """Запустить проверку статических импортов CLI против границы QC."""

    violations = collect_qc_boundary_violations()
    if not violations:
        typer.echo("Граница CLI↔QC соблюдена, нарушений не обнаружено.")
        raise typer.Exit(code=0)

    typer.secho(
        "Обнаружены нарушения границы CLI↔QC:",
        err=True,
        fg=typer.colors.RED,
    )
    for violation in violations:
        typer.secho(
            f"- {violation.source_path}: {violation.format_chain()}",
            err=True,
            fg=typer.colors.RED,
        )
    raise typer.Exit(code=1)


def run() -> None:
    """Запустить Typer-приложение."""

    run_app(app)


if __name__ == "__main__":
    run()


