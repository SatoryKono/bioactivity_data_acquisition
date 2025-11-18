"""Main Typer application for the BioETL CLI and batch helpers.

Console entry points (``python -m bioetl.cli.cli_app`` и установленные
скрипты ``bioetl``) используют функции из этого модуля. Здесь создаётся Typer
приложение, материализуются `COMMAND_REGISTRY`/`PIPELINE_REGISTRY`,
регистрируются как одиночные пайплайны, так и вспомогательные группы вроде
``bioetl run-chembl-all`` и `bioetl config ...`. Модуль является единственной
точкой входа для CLI после удаления ``bioetl.cli.main``.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable, Iterable, Mapping
from collections.abc import Mapping as MappingType
from pathlib import Path
from typing import Any, cast

import yaml

from bioetl.cli.cli_command import create_pipeline_command
from bioetl.cli.cli_entrypoint import TyperApp, run_app
from bioetl.cli.cli_entrypoint import create_app as create_typer_app
from bioetl.cli.cli_registry import (
    COMMAND_REGISTRY,
    PIPELINE_REGISTRY,
    CommandConfig,
    PipelineCommandSpec,
)
from bioetl.cli.run_chembl_all import run_chembl_all_command
from bioetl.config.runtime import Config as RuntimeConfig
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.core.runtime import cli_feedback
from bioetl.core.runtime.cli_pipeline_runner import (
    ConfigLoadError,
    EnvironmentSetupError,
    PipelineCommandOptions,
    PipelineConfigFactory,
    parse_set_overrides,
    validate_config_path,
    validate_output_dir,
)

typer = cast(Any, importlib.import_module("typer"))

_log = UnifiedLogger.get(__name__)

__all__ = ["app", "create_app", "run"]


def _render_config_payload(payload: MappingType[str, Any], format_name: str) -> str:
    """Serialize a configuration mapping into YAML or JSON."""

    normalized = format_name.lower()
    if normalized == "yaml":
        return yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
    if normalized == "json":
        return json.dumps(payload, ensure_ascii=False, indent=2)
    msg = "--format must be either 'yaml' or 'json'"
    raise ValueError(msg)


def create_app(
    command_registry: Mapping[str, Callable[[], Any]] | None = None,
    pipeline_specs: Iterable[PipelineCommandSpec] | None = None,
) -> TyperApp:
    """Create and configure the Typer application with all registered commands."""
    registry = dict(command_registry or COMMAND_REGISTRY)
    specs: tuple[PipelineCommandSpec, ...] = tuple(pipeline_specs or PIPELINE_REGISTRY)
    known_names: set[str] = {name for spec in specs for name in (spec.code, *spec.aliases)}

    warning_messages: list[str] = []

    app = create_typer_app(
        name="bioetl",
        help_text="BioETL command-line interface for executing ETL pipelines.",
    )

    config_app = typer.Typer(
        help="Configuration helpers (inspection, validation, metadata).",
        no_args_is_help=True,
    )
    app.add_typer(config_app, name="config")

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

    @app.command(name="run-chembl-all")
    def run_chembl_all_wrapper(
        output_root: Path = typer.Option(
            ...,
            "--output-root",
            "-o",
            help="Корневая директория для артефактов всех пайплайнов",
        ),
        configs_dir: Path = typer.Option(
            Path("configs"),
            "--configs-dir",
            help="Корневая директория конфигов",
        ),
        limit: int | None = typer.Option(
            None,
            "--limit",
            help="Ограничить количество строк для каждого пайплайна (для тестирования)",
            min=1,
        ),
        extended: bool = typer.Option(
            False,
            "--extended",
            help="Включить расширенные QC-артефакты",
        ),
        golden: Path | None = typer.Option(
            None,
            "--golden",
            help="Путь к golden-набору для сравнения (опционально)",
            exists=False,
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
            "-v",
            help="Включить подробное логирование",
        ),
        dry_run: bool = typer.Option(
            False,
            "--dry-run",
            "-d",
            help="Проверить конфигурацию без выполнения",
        ),
    ) -> None:
        """Последовательно запустить все ChEMBL-пайплайны и собрать единый отчёт."""
        run_chembl_all_command(
            output_root=output_root,
            configs_dir=configs_dir,
            limit=limit,
            extended=extended,
            golden=golden,
            verbose=verbose,
            dry_run=dry_run,
        )

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
            materialization_root
            if materialization_root is not None
            else Path("data/output") / pipeline
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

    @config_app.command(name="inspect")
    def config_inspect(
        config: Path = typer.Option(
            ...,
            "--config",
            "-c",
            help="Path to the YAML configuration file",
            exists=False,
        ),
        output_dir: Path = typer.Option(
            Path("data/output/_inspect"),
            "--output-dir",
            "-o",
            help="Output directory used to resolve materialization paths",
        ),
        output_format: str = typer.Option(
            "yaml",
            "--format",
            "-f",
            help="Serialization format for the merged configuration (yaml/json)",
        ),
        set_overrides: list[str] = typer.Option(
            [],
            "--set",
            "-S",
            help="Override individual keys using dotted notation (KEY=VALUE)",
        ),
        sample: int | None = typer.Option(
            None,
            "--sample",
            help="Deterministically sample N rows to preview transforms",
            min=1,
        ),
        limit: int | None = typer.Option(
            None,
            "--limit",
            help="Process at most N rows during inspection",
            min=1,
        ),
        extended: bool = typer.Option(
            False,
            "--extended",
            help="Mark the config as extended to preview QC/correlation artifacts",
        ),
        fail_on_schema_drift: bool = typer.Option(
            True,
            "--fail-on-schema-drift/--allow-schema-drift",
            help="Control strict column validation when materializing the config",
        ),
        validate_columns: bool = typer.Option(
            True,
            "--validate-columns/--no-validate-columns",
            help="Toggle Pandera column validation flag",
        ),
        golden: Path | None = typer.Option(
            None,
            "--golden",
            help="Optional path to a golden dataset for metadata previews",
            exists=False,
        ),
        input_file: Path | None = typer.Option(
            None,
            "--input-file",
            "-i",
            help="Optional seed file wired into config.cli.input_file",
            exists=False,
        ),
    ) -> None:
        """Load, merge, and print a typed configuration without executing a pipeline."""

        if limit is not None and sample is not None:
            raise typer.BadParameter("--limit and --sample are mutually exclusive")

        try:
            config_path = validate_config_path(config)
        except FileNotFoundError as exc:
            raise typer.BadParameter(str(exc)) from exc

        try:
            resolved_output_dir = validate_output_dir(output_dir)
        except OSError as exc:
            raise typer.BadParameter(str(exc)) from exc

        try:
            cli_overrides = parse_set_overrides(set_overrides) if set_overrides else {}
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc

        options = PipelineCommandOptions(
            config_path=config_path,
            output_dir=resolved_output_dir,
            dry_run=True,
            verbose=False,
            set_overrides=cli_overrides,
            sample=sample,
            limit=limit,
            extended=extended,
            fail_on_schema_drift=fail_on_schema_drift,
            validate_columns=validate_columns,
            golden=golden,
            input_file=input_file,
        )

        factory = PipelineConfigFactory()
        try:
            pipeline_config = factory.create(options)
        except EnvironmentSetupError as exc:
            cli_feedback.emit_error(f"Environment validation failed: {exc}")
            raise typer.Exit(code=2) from exc
        except ConfigLoadError as exc:
            if exc.missing_reference:
                message = f"Configuration file or referenced profile not found: {exc}"
            else:
                message = f"Configuration validation failed: {exc}"
            cli_feedback.emit_error(message)
            raise typer.Exit(code=2) from exc

        cli_feedback.emit_section("Configuration summary")
        cli_feedback.emit_kv("config_path", str(config_path), indent=1)
        cli_feedback.emit_kv("pipeline", pipeline_config.pipeline.name, indent=1)
        cli_feedback.emit_kv("version", pipeline_config.pipeline.version, indent=1)
        owner = pipeline_config.pipeline.owner or "n/a"
        cli_feedback.emit_kv("owner", owner, indent=1)
        cli_feedback.emit_kv("output_dir", str(resolved_output_dir), indent=1)

        if pipeline_config.cli.profiles:
            cli_feedback.emit_line("Profiles:", indent=1)
            for profile in pipeline_config.cli.profiles:
                cli_feedback.emit_line(f"- {profile}", indent=2)
        if pipeline_config.cli.environment_profiles:
            cli_feedback.emit_line("Environment profiles:", indent=1)
            for profile in pipeline_config.cli.environment_profiles:
                cli_feedback.emit_line(f"- {profile}", indent=2)
        if pipeline_config.cli.environment:
            cli_feedback.emit_kv("environment", pipeline_config.cli.environment, indent=1)

        cli_feedback.emit_line("Domain toggles:", indent=1)
        cli_feedback.emit_kv(
            "postprocess.correlation.enabled",
            pipeline_config.postprocess.correlation.enabled,
            indent=2,
        )
        cli_feedback.emit_kv(
            "fallbacks.enabled",
            pipeline_config.fallbacks.enabled,
            indent=2,
        )
        cli_feedback.emit_kv(
            "fallbacks.max_depth",
            pipeline_config.fallbacks.max_depth or "unbounded",
            indent=2,
        )

        cli_feedback.emit_section("Normalized configuration")
        payload = pipeline_config.model_dump(mode="json")
        try:
            serialized = _render_config_payload(payload, output_format)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        typer.echo(serialized)

    registered_names: set[str] = set()

    def _register_command(name: str, config: CommandConfig) -> None:
        command_func = create_pipeline_command(
            pipeline_class=config.pipeline_class,
            command_config=config,
        )
        app.command(name=name, help=config.description)(command_func)
        registered_names.add(name)

    for spec in specs:
        command_name = spec.code
        build_config_func = registry.get(command_name)
        if build_config_func is None:
            warning_message = f"Command '{command_name}' missing from registry definition"
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
                warning_message = f"Alias '{alias}' missing from registry definition"
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
