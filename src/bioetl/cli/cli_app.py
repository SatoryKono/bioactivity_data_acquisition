"""Main Typer application for BioETL CLI.

Console entry points should target :func:`bioetl.cli.cli_app.run`.
Legacy module :mod:`bioetl.cli.main` was removed; use this module exclusively.

This module creates the Typer application and registers all pipeline commands
from the static registry.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Callable, cast

from bioetl.cli.cli_command import create_pipeline_command
from bioetl.cli.cli_entrypoint import TyperApp, run_app
from bioetl.cli.cli_entrypoint import create_app as create_typer_app
from bioetl.cli.cli_registry import (
    COMMAND_REGISTRY,
    PIPELINE_REGISTRY,
    CommandConfig,
    PipelineCommandSpec,
)
from bioetl.config.runtime import Config as RuntimeConfig
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.core.runtime import cli_feedback

typer = cast(Any, importlib.import_module("typer"))

_log = UnifiedLogger.get(__name__)

__all__ = ["app", "create_app", "run"]


def create_app(
    command_registry: Mapping[str, Callable[[], Any]] | None = None,
    pipeline_specs: Iterable[PipelineCommandSpec] | None = None,
) -> TyperApp:
    """Create and configure the Typer application with all registered commands."""
    registry = dict(command_registry or COMMAND_REGISTRY)
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
        """List all available pipeline commands."""
        cli_feedback.emit_section("Registered pipeline commands")
        for spec in sorted(specs, key=lambda item: item.code):
            command_name = spec.code
            build_config_func = registry.get(command_name)
            if build_config_func is None:
                warning = f"Command '{command_name}' not found in registry definition"
                warning_messages.append(warning)
                cli_feedback.emit_list_item(command_name, "registry entry missing")
                continue
            try:
                config = build_config_func()
            except NotImplementedError:
                cli_feedback.emit_list_item(command_name, "not implemented")
                continue
            except Exception as exc:  # noqa: BLE001
                _log.error(
                    LogEvents.CLI_REGISTRY_LOOKUP_FAILED,
                    command=command_name,
                    error=str(exc),
                    exc_info=True,
                )
                cli_feedback.emit_list_item(command_name, f"ERROR: {exc}")
                continue

            alias_suffix = f" (aliases: {', '.join(spec.aliases)})" if spec.aliases else ""
            cli_feedback.emit_list_item(
                command_name,
                f"{config.description}{alias_suffix}",
            )

        extra_names = sorted(set(registry.keys()) - known_names)
        for command_name in extra_names:
            try:
                config = registry[command_name]()
            except NotImplementedError:
                cli_feedback.emit_list_item(command_name, "not implemented")
                continue
            except Exception as exc:  # noqa: BLE001
                _log.error(
                    LogEvents.CLI_REGISTRY_LOOKUP_FAILED,
                    command=command_name,
                    error=str(exc),
                    exc_info=True,
                )
                cli_feedback.emit_list_item(command_name, f"ERROR: {exc}")
                continue
            cli_feedback.emit_list_item(command_name, config.description)

        for message in warning_messages:
            cli_feedback.emit_warning(message)

    @app.command(name="qc")
    def qc_info(
        pipeline: str = typer.Option(
            None,
            "--pipeline",
            "-p",
            help="Pipeline code used to resolve thresholds and report templates.",
        ),
        runtime_config: Path = typer.Option(
            Path("configs/default.yml"),
            "--runtime-config",
            help="Path to the runtime configuration file.",
        ),
        materialization_root: Path | None = typer.Option(
            None,
            "--materialization-root",
            "-m",
            help=(
                "Materialization root used to expand QC report templates. "
                "Defaults to data/output/<pipeline> when --pipeline is provided."
            ),
        ),
    ) -> None:
        try:
            runtime_settings = RuntimeConfig.load(runtime_config)
        except FileNotFoundError as exc:
            cli_feedback.emit_error(f"Runtime configuration not found: {runtime_config}")
            raise typer.Exit(code=2) from exc
        except ValueError as exc:
            cli_feedback.emit_error(f"Runtime configuration is invalid: {exc}")
            raise typer.Exit(code=2) from exc

        cli_feedback.emit_section("QC configuration summary")
        base_thresholds = runtime_settings.thresholds_for(None)
        cli_feedback.emit_line("Base thresholds:", indent=1)
        for key, value in sorted(base_thresholds.items()):
            cli_feedback.emit_kv(key, value, indent=2)
        cli_feedback.emit_line(
            f"Fail on QC violation: {runtime_settings.qc.fail_on_threshold_violation}",
            indent=1,
        )

        if not pipeline:
            cli_feedback.emit_line(
                "Use --pipeline to view pipeline-specific thresholds and report layout.",
                indent=1,
            )
            return

        pipeline_thresholds = runtime_settings.thresholds_for(pipeline)
        cli_feedback.emit_line(f"Thresholds for {pipeline}:", indent=1)
        for key, value in sorted(pipeline_thresholds.items()):
            cli_feedback.emit_kv(key, value, indent=2)

        effective_root = (
            materialization_root if materialization_root is not None else Path("data/output") / pipeline
        )
        report_options = runtime_settings.reports_for(
            pipeline=pipeline,
            materialization_root=effective_root,
        )
        cli_feedback.emit_line("QC report templates:", indent=1)
        cli_feedback.emit_kv("directory", report_options.directory, indent=2)
        cli_feedback.emit_kv("quality_template", report_options.quality_template, indent=2)
        cli_feedback.emit_kv("correlation_template", report_options.correlation_template, indent=2)
        cli_feedback.emit_kv("metrics_template", report_options.metrics_template, indent=2)

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
                f"Command '{command_name}' missing from registry definition"
            )
            warning_messages.append(warning_message)
            cli_feedback.emit_warning(warning_message)
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
            warning_message = f"Command '{command_name}' not loaded ({exc})"
            warning_messages.append(warning_message)
            cli_feedback.emit_warning(warning_message)
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
            warning_message = f"Command '{command_name}' not loaded ({exc})"
            warning_messages.append(warning_message)
            cli_feedback.emit_warning(warning_message)
            continue

        for alias in spec.aliases:
            if alias in registered_names:
                continue
            alias_builder = registry.get(alias)
            if alias_builder is None:
                warning_message = (
                    f"Alias '{alias}' missing from registry definition"
                )
                warning_messages.append(warning_message)
                cli_feedback.emit_warning(warning_message)
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
                warning_message = f"Alias '{alias}' not loaded ({exc})"
                warning_messages.append(warning_message)
                cli_feedback.emit_warning(warning_message)
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
                warning_message = f"Alias '{alias}' not loaded ({exc})"
                warning_messages.append(warning_message)
                cli_feedback.emit_warning(warning_message)
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
            warning_message = f"Command '{command_name}' not loaded ({exc})"
            warning_messages.append(warning_message)
            cli_feedback.emit_warning(warning_message)
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
            warning_message = f"Command '{command_name}' not loaded ({exc})"
            warning_messages.append(warning_message)
            cli_feedback.emit_warning(warning_message)

    return app


app = create_app()


def run() -> None:
    """Entry point for CLI application."""
    run_app(app)


if __name__ == "__main__":
    run()


