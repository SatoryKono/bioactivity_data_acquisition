"""CLI command ``bioetl-schema-guard``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.tools import emit_tool_error, exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.core.runtime.cli_errors import CLI_ERROR_CONFIG, CLI_ERROR_INTERNAL
from bioetl.tools.schema_guard import run_schema_guard as run_schema_guard_sync

typer: Any = get_typer()

__all__ = ["app", "main", "run", "run_schema_guard"]
run_schema_guard = run_schema_guard_sync

def main() -> None:
    """Run configuration and schema validation."""

    try:
        results, registry_errors, report_path = run_schema_guard()
    except Exception as exc:  # noqa: BLE001
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Schema guard execution failed: {exc}",
            context={
                "command": "bioetl-schema-guard",
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

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
        emit_tool_error(
            template=CLI_ERROR_CONFIG,
            message=(
                "Schema guard detected invalid configurations or registry errors. "
                f"Report: {report_path.resolve()}"
            ),
            context={
                "command": "bioetl-schema-guard",
                "invalid_config_count": len(invalid_configs),
                "registry_error_count": len(registry_errors),
                "report_path": str(report_path.resolve()),
            },
        )

    typer.echo(f"All configurations are valid. Report: {report_path.resolve()}")
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-schema-guard",
    help_text="Validate pipeline configs and the Pandera registry",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()
