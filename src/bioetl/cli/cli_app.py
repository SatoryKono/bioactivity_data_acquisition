"""Main Typer application for BioETL CLI.

Console entry points should target :func:`bioetl.cli.cli_app.run`.
Legacy module :mod:`bioetl.cli.main` was removed; use this module exclusively.

This module creates the Typer application and registers all pipeline commands
from the static registry.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterable, Mapping
from typing import Any, Callable, cast

from bioetl.cli.cli_command import create_pipeline_command
from bioetl.cli.cli_entrypoint import (
    TyperApp,
    run_app,
)
from bioetl.cli.cli_entrypoint import (
    create_app as create_typer_app,
)
from bioetl.cli.cli_registry import (
    COMMAND_REGISTRY,
    PIPELINE_REGISTRY,
    TOOL_COMMANDS,
    CommandConfig,
    PipelineCommandSpec,
    ToolCommandConfig,
)
from bioetl.core.logging import LogEvents, UnifiedLogger

typer = cast(Any, importlib.import_module("typer"))

_log = UnifiedLogger.get(__name__)

__all__ = ["app", "create_app", "run"]


def create_app(
    command_registry: Mapping[str, Callable[[], Any]] | None = None,
    tool_commands: Mapping[str, ToolCommandConfig] | None = None,
    pipeline_specs: Iterable[PipelineCommandSpec] | None = None,
) -> TyperApp:
    """Create and configure the Typer application with all registered commands."""
    registry = dict(command_registry or COMMAND_REGISTRY)
    tools = dict(tool_commands or TOOL_COMMANDS)
    specs: tuple[PipelineCommandSpec, ...] = tuple(pipeline_specs or PIPELINE_REGISTRY)
    known_names: set[str] = {
        name for spec in specs for name in (spec.code, *spec.aliases)
    }

    warning_messages: list[str] = []

    app = create_typer_app(
        name="bioetl",
        help_text="BioETL command-line interface for executing ETL pipelines.",
    )

    @app.command(name="list")
    def list_commands() -> None:
        """List all available pipeline and tool commands."""
        typer.echo("[bioetl-cli] Registered pipeline commands:")
        for spec in sorted(specs, key=lambda item: item.code):
            command_name = spec.code
            build_config_func = registry.get(command_name)
            if build_config_func is None:
                warning = (
                    f"[bioetl-cli] WARN: Command '{command_name}' not found in registry definition"
                )
                warning_messages.append(warning)
                typer.echo(f"  {command_name:<20} - registry entry missing")
                continue
            try:
                config = build_config_func()
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

            alias_suffix = f" (aliases: {', '.join(spec.aliases)})" if spec.aliases else ""
            typer.echo(f"  {command_name:<20} - {config.description}{alias_suffix}")

        if tools:
            typer.echo("[bioetl-cli] Registered utility commands:")
            for tool_name, tool_config in sorted(tools.items()):
                typer.echo(f"  {tool_name:<20} - {tool_config.description}")

        extra_names = sorted(set(registry.keys()) - known_names)
        for command_name in extra_names:
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

        for message in warning_messages:
            typer.echo(message)

    registered_names: set[str] = set()

    def _register_command(name: str, config: CommandConfig) -> None:
        command_func = create_pipeline_command(
            pipeline_class=config.pipeline_class,
            command_config=config,
        )
        app.command(name=name)(command_func)
        registered_names.add(name)

    for spec in specs:
        command_name = spec.code
        build_config_func = registry.get(command_name)
        if build_config_func is None:
            warning_message = (
                f"[bioetl-cli] WARN: Command '{command_name}' missing from registry definition"
            )
            warning_messages.append(warning_message)
            typer.echo(warning_message)
            typer.echo(warning_message, err=True)
            continue
        try:
            command_config = build_config_func()
        except NotImplementedError:
            continue
        except Exception as exc:
            _log.error(
                LogEvents.CLI_COMMAND_REGISTRATION_FAILED,
                command=command_name,
                error=str(exc),
                exc_info=True,
            )
            warning_message = f"[bioetl-cli] WARN: Command '{command_name}' not loaded ({exc})"
            warning_messages.append(warning_message)
            typer.echo(warning_message)
            typer.echo(warning_message, err=True)
            continue

        try:
            _register_command(command_name, command_config)
        except Exception as exc:  # noqa: BLE001
            _log.error(
                LogEvents.CLI_COMMAND_REGISTRATION_FAILED,
                command=command_name,
                error=str(exc),
                exc_info=True,
            )
            warning_message = f"[bioetl-cli] WARN: Command '{command_name}' not loaded ({exc})"
            warning_messages.append(warning_message)
            typer.echo(warning_message)
            typer.echo(warning_message, err=True)
            continue

        for alias in spec.aliases:
            if alias in registered_names:
                continue
            alias_builder = registry.get(alias)
            if alias_builder is None:
                warning_message = (
                    f"[bioetl-cli] WARN: Alias '{alias}' missing from registry definition"
                )
                warning_messages.append(warning_message)
                typer.echo(warning_message)
                typer.echo(warning_message, err=True)
                continue
            try:
                alias_config = alias_builder()
            except NotImplementedError:
                continue
            except Exception as exc:
                _log.error(
                    LogEvents.CLI_COMMAND_REGISTRATION_FAILED,
                    command=alias,
                    error=str(exc),
                    exc_info=True,
                )
                warning_message = f"[bioetl-cli] WARN: Alias '{alias}' not loaded ({exc})"
                warning_messages.append(warning_message)
                typer.echo(warning_message)
                typer.echo(warning_message, err=True)
                continue
            try:
                _register_command(alias, alias_config)
            except Exception as exc:  # noqa: BLE001
                _log.error(
                    LogEvents.CLI_COMMAND_REGISTRATION_FAILED,
                    command=alias,
                    error=str(exc),
                    exc_info=True,
                )
                warning_message = f"[bioetl-cli] WARN: Alias '{alias}' not loaded ({exc})"
                warning_messages.append(warning_message)
                typer.echo(warning_message)
                typer.echo(warning_message, err=True)
                continue

    extra_entries = sorted(set(registry.keys()) - registered_names)
    for command_name in extra_entries:
        build_config_func = registry[command_name]
        try:
            command_config = build_config_func()
        except NotImplementedError:
            continue
        except Exception as exc:
            _log.error(
                LogEvents.CLI_COMMAND_REGISTRATION_FAILED,
                command=command_name,
                error=str(exc),
                exc_info=True,
            )
            warning_message = f"[bioetl-cli] WARN: Command '{command_name}' not loaded ({exc})"
            warning_messages.append(warning_message)
            typer.echo(warning_message)
            typer.echo(warning_message, err=True)
            continue
        if command_name in registered_names:
            continue
        try:
            _register_command(command_name, command_config)
        except Exception as exc:  # noqa: BLE001
            _log.error(
                LogEvents.CLI_COMMAND_REGISTRATION_FAILED,
                command=command_name,
                error=str(exc),
                exc_info=True,
            )
            warning_message = f"[bioetl-cli] WARN: Command '{command_name}' not loaded ({exc})"
            warning_messages.append(warning_message)
            typer.echo(warning_message)
            typer.echo(warning_message, err=True)
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
            warning_message = f"[bioetl-cli] WARN: Tool '{tool_name}' not loaded ({exc})"
            warning_messages.append(warning_message)
            typer.echo(warning_message)
            typer.echo(warning_message, err=True)

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
