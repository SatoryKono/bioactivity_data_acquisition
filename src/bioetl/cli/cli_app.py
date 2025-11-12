"""Main Typer application for BioETL CLI.

Console entry points should target :func:`bioetl.cli.cli_app.run`.
Legacy module :mod:`bioetl.cli.main` was removed; use this module exclusively.

This module creates the Typer application and registers all pipeline commands
from the static registry.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import Any, Callable, cast

from bioetl.cli.cli_command import create_pipeline_command
from bioetl.cli.cli_registry import COMMAND_REGISTRY, TOOL_COMMANDS, ToolCommandConfig
from bioetl.cli.cli_runner import run_app
from bioetl.cli.tools._typer import TyperApp
from bioetl.cli.tools._typer import create_app as create_typer_app
from bioetl.core.log_events import LogEvents
from bioetl.core.logger import UnifiedLogger

typer = cast(Any, importlib.import_module("typer"))

_log = UnifiedLogger.get(__name__)

__all__ = ["app", "create_app", "run"]


def create_app(
    command_registry: Mapping[str, Callable[[], Any]] | None = None,
    tool_commands: Mapping[str, ToolCommandConfig] | None = None,
) -> TyperApp:
    """Create and configure the Typer application with all registered commands."""
    registry = dict(command_registry or COMMAND_REGISTRY)
    tools = dict(tool_commands or TOOL_COMMANDS)

    app = create_typer_app(
        name="bioetl",
        help_text="BioETL command-line interface for executing ETL pipelines.",
    )

    @app.command(name="list")
    def list_commands() -> None:
        """List all available pipeline and tool commands."""
        typer.echo("[bioetl-cli] Registered pipeline commands:")
        for command_name in sorted(registry.keys()):
            try:
                config = registry[command_name]()
            except NotImplementedError:
                typer.echo(f"  {command_name:<20} - not implemented")
                continue
            except Exception as exc:  # noqa: BLE001
                _log.error(
                    LogEvents.CLI_REGISTRY_LOOKUP_FAILED,
                    command=command_name,
                    error=str(exc),
                    exc_info=True,
                )
                typer.echo(f"  {command_name:<20} - ERROR: {exc}")
                continue

            typer.echo(f"  {command_name:<20} - {config.description}")

        if tools:
            typer.echo("[bioetl-cli] Registered utility commands:")
            for tool_name, tool_config in sorted(tools.items()):
                typer.echo(f"  {tool_name:<20} - {tool_config.description}")

    for command_name, build_config_func in registry.items():
        try:
            command_config = build_config_func()
            command_func = create_pipeline_command(
                pipeline_class=command_config.pipeline_class,
                command_config=command_config,
            )
            app.command(name=command_name)(command_func)
        except NotImplementedError:
            continue
        except Exception as exc:
            _log.error(
                LogEvents.CLI_COMMAND_REGISTRATION_FAILED,
                command=command_name,
                error=str(exc),
                exc_info=True,
            )
            typer.echo(
                f"[bioetl-cli] WARN: Command '{command_name}' not loaded ({exc})",
                err=True,
            )

    for tool_name, tool_config in tools.items():
        try:
            entrypoint = _load_tool_entrypoint(tool_config)
            app.command(name=tool_name)(entrypoint)
        except Exception as exc:  # noqa: BLE001
            _log.error(
                LogEvents.CLI_COMMAND_REGISTRATION_FAILED,
                command=tool_name,
                error=str(exc),
                exc_info=True,
            )
            typer.echo(
                f"[bioetl-cli] WARN: Tool '{tool_name}' not loaded ({exc})",
                err=True,
            )

    return app


def _load_tool_entrypoint(tool_config: ToolCommandConfig) -> Callable[..., None]:
    """Dynamically load a tool entrypoint without importing QC modules into CLI."""
    module = importlib.import_module(tool_config.module)
    attribute = getattr(module, tool_config.attribute)
    if callable(attribute):
        return cast(Callable[..., None], attribute)
    msg = (
        f"Attribute '{tool_config.attribute}' from '{tool_config.module}' is not callable."
    )
    raise TypeError(msg)


app = create_app()


def run() -> None:
    """Entry point for CLI application."""
    run_app(app)


if __name__ == "__main__":
    run()
