"""CLI command ``bioetl-semantic-diff``."""

from __future__ import annotations

import importlib
from typing import Any, cast

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.tools.semantic_diff import run_semantic_diff as run_semantic_diff_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "run_semantic_diff"]

run_semantic_diff = run_semantic_diff_sync


app: TyperApp = create_app(
    name="bioetl-semantic-diff",
    help_text="Compare documentation and code to produce a diff",
)


@app.command()
def main() -> None:
    """Run the semantic diff workflow."""

    try:
        report_path = run_semantic_diff()
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    typer.echo(f"Semantic diff report written to: {report_path.resolve()}")
    exit_with_code(0)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

