"""CLI command ``bioetl-schema-guard``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_schema_guard as cli_schema_guard_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_CONFIG, CLI_ERROR_INTERNAL

_LOGIC_EXPORTS = getattr(cli_schema_guard_impl, "__all__", [])
globals().update({symbol: getattr(cli_schema_guard_impl, symbol) for symbol in _LOGIC_EXPORTS})
__all__ = [* _LOGIC_EXPORTS, "app", "cli_main", "run"]

typer: Any = get_typer()


def cli_main() -> None:
    """Run configuration and schema validation."""

    try:
        results, registry_errors, report_path = cli_schema_guard_impl.run_schema_guard()
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
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
        CliCommandBase.emit_error(
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
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-schema-guard",
    help_text="Validate pipeline configs and the Pandera registry",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()