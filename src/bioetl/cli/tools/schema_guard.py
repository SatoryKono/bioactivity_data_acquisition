"""CLI command ``bioetl-schema-guard``."""

from __future__ import annotations

import importlib
from typing import Any, cast

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools._typer import TyperApp, create_app, run_app
from bioetl.tools.schema_guard import run_schema_guard as run_schema_guard_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "run_schema_guard"]
run_schema_guard = run_schema_guard_sync

app: TyperApp = create_app(
    name="bioetl-schema-guard",
    help_text="Validate pipeline configs and the Pandera registry",
)


@app.command()
def main() -> None:
    """Run configuration and schema validation."""

    try:
        results, registry_errors, report_path = run_schema_guard()
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    invalid_configs = [
        name for name, payload in results.items() if not payload.get("valid", False)
    ]

    if invalid_configs or registry_errors:
        typer.secho(
            "Issues detected in configurations or schema registry:",
            fg=typer.colors.RED,
        )
        if invalid_configs:
            typer.echo(" - Invalid configurations: " + ", ".join(sorted(invalid_configs)))
        if registry_errors:
            typer.echo(" - Schema registry errors:")
            for error in registry_errors:
                typer.echo(f"   * {error}")
        typer.echo(f"Report: {report_path.resolve()}")
        exit_with_code(1)

    typer.echo(f"All configurations are valid. Report: {report_path.resolve()}")
    exit_with_code(0)


def run() -> None:
    """Execute the Typer application."""
    run_app(app)


if __name__ == "__main__":
    run()
