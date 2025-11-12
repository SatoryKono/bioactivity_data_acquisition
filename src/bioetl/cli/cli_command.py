"""Common command options and pipeline command factory for BioETL CLI."""

from __future__ import annotations

import importlib
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, NoReturn, Protocol, cast
from zoneinfo import ZoneInfo

import typer

from bioetl.config.environment import apply_runtime_overrides, load_environment_settings
from bioetl.config.loader import load_config
from bioetl.core.cli_base import CliCommandBase
from bioetl.core.log_events import LogEvents
from bioetl.core.logger import LoggerConfig, UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.pipelines.errors import (
    PipelineError,
    PipelineHTTPError,
    PipelineNetworkError,
    PipelineTimeoutError,
)

__all__ = ["create_pipeline_command", "CommonOptions"]


class LoggerProtocol(Protocol):
    """Минимальный контракт логгера, используемый фабрикой CLI."""

    def info(self, event: str, /, **context: Any) -> Any:
        ...

    def error(self, event: str, /, **context: Any) -> Any:
        ...


class CommonOptions:
    """Common options shared by all pipeline commands."""

    def __init__(
        self,
        *,
        config: Path,
        output_dir: Path,
        dry_run: bool = False,
        verbose: bool = False,
        set_overrides: list[str] | None = None,
        sample: int | None = None,
        limit: int | None = None,
        extended: bool = False,
        fail_on_schema_drift: bool = True,
        validate_columns: bool = True,
        golden: Path | None = None,
    ) -> None:
        self.config = config
        self.output_dir = output_dir
        self.dry_run = dry_run
        self.verbose = verbose
        self.set_overrides = set_overrides or []
        self.sample = sample
        self.limit = limit
        self.extended = extended
        self.fail_on_schema_drift = fail_on_schema_drift
        self.validate_columns = validate_columns
        self.golden = golden


class PipelineCliCommand(CliCommandBase):
    """Реализация Typer-команды для запуска пайплайна."""

    def __init__(
        self,
        *,
        pipeline_module_name: str,
        pipeline_class_name: str,
        command_config: Any,
    ) -> None:
        super().__init__(logger=UnifiedLogger.get(__name__))
        self._pipeline_module_name = pipeline_module_name
        self._pipeline_class_name = pipeline_class_name
        self._command_config = command_config
        self._run_id: str | None = None

    def handle(
        self,
        *,
        config: Path,
        output_dir: Path,
        dry_run: bool,
        verbose: bool,
        set_overrides: list[str],
        sample: int | None,
        limit: int | None,
        extended: bool,
        fail_on_schema_drift: bool,
        validate_columns: bool,
        golden: Path | None,
        input_file: Path | None,
    ) -> None:
        if limit is not None and sample is not None:
            raise typer.BadParameter("--limit and --sample are mutually exclusive")

        try:
            env_settings = load_environment_settings()
        except ValueError as exc:
            self.emit_error("E002", f"Environment validation failed: {exc}")
            self.exit(2)

        apply_runtime_overrides(env_settings)

        _validate_config_path(config)
        _validate_output_dir(output_dir)

        cli_overrides: dict[str, Any] = {}
        if set_overrides:
            cli_overrides = _parse_set_overrides(set_overrides)

        try:
            pipeline_config = load_config(
                config_path=config,
                cli_overrides=cli_overrides,
                include_default_profiles=True,
            )
        except FileNotFoundError as exc:
            self.emit_error(
                "E002",
                f"Configuration file or referenced profile not found: {exc}",
            )
            self.exit(2)
        except ValueError as exc:
            self.emit_error("E002", f"Configuration validation failed: {exc}")
            self.exit(2)

        pipeline_config.cli.dry_run = dry_run
        if limit is not None:
            pipeline_config.cli.limit = limit
        if sample is not None:
            pipeline_config.cli.sample = sample
        pipeline_config.cli.extended = extended
        if golden is not None:
            pipeline_config.cli.golden = str(golden)
        if input_file is not None:
            pipeline_config.cli.input_file = str(input_file)
        pipeline_config.cli.verbose = verbose
        pipeline_config.cli.fail_on_schema_drift = fail_on_schema_drift
        pipeline_config.cli.validate_columns = validate_columns
        if not validate_columns:
            pipeline_config.validation.strict = False

        pipeline_config.materialization.root = str(output_dir)

        log_level = "DEBUG" if verbose else "INFO"
        UnifiedLogger.configure(LoggerConfig(level=log_level))

        if dry_run:
            typer.echo("Configuration validated successfully (dry-run mode)")
            self.exit(0)

        run_id = str(uuid.uuid4())
        self._run_id = run_id

        timezone_name = pipeline_config.determinism.environment.timezone
        try:
            tz = ZoneInfo(timezone_name)
        except Exception:  # pragma: no cover - fallback path
            tz = ZoneInfo("UTC")
        pipeline_config.cli.date_tag = pipeline_config.cli.date_tag or datetime.now(tz).strftime(
            "%Y%m%d"
        )

        log = self.logger
        log.info(
            LogEvents.CLI_RUN_START,
            pipeline=self._command_config.name,
            config=str(config),
            output_dir=str(output_dir),
            run_id=run_id,
            dry_run=dry_run,
            limit=limit,
            extended=extended,
        )

        pipeline_factory = _resolve_pipeline_class(
            module_name=self._pipeline_module_name,
            class_name=self._pipeline_class_name,
            log=log,
            run_id=run_id,
            command_name=self._command_config.name,
        )
        pipeline = pipeline_factory(pipeline_config, run_id)

        postprocess_config = getattr(pipeline_config, "postprocess", None)
        correlation_section = getattr(postprocess_config, "correlation", None)
        correlation_enabled = bool(getattr(correlation_section, "enabled", False))
        effective_extended = bool(extended or pipeline_config.cli.extended)
        run_kwargs: dict[str, Any] = {
            "extended": effective_extended,
            "include_correlation": effective_extended or correlation_enabled,
            "include_qc_metrics": effective_extended,
        }

        try:
            result = pipeline.run(
                Path(output_dir),
                **run_kwargs,
            )
        except typer.Exit:
            raise
        except Exception as exc:  # noqa: BLE001
            self.handle_exception(exc)
            return

        log.info(
            LogEvents.CLI_RUN_FINISH,
            run_id=run_id,
            pipeline=self._command_config.name,
            dataset=str(result.write_result.dataset),
            duration_ms=sum(result.stage_durations_ms.values()),
        )

        typer.echo(f"Pipeline completed successfully: {result.write_result.dataset}")
        self.exit(0)

    def handle_exception(self, exc: Exception) -> NoReturn:
        run_id = self._run_id or "unknown"
        _handle_pipeline_exception(
            exc=exc,
            log=self.logger,
            run_id=run_id,
            command_name=self._command_config.name,
        )
        raise typer.Exit(code=self.exit_code_error)

def _parse_set_overrides(set_overrides: list[str]) -> dict[str, Any]:
    """Parse --set KEY=VALUE flags into a dictionary."""
    parsed: dict[str, Any] = {}
    for override in set_overrides:
        if "=" not in override:
            msg = f"Invalid --set format: {override}. Expected KEY=VALUE"
            raise typer.BadParameter(msg)
        key, value = override.split("=", 1)
        parsed[key] = value
    return parsed


def _validate_config_path(config_path: Path) -> None:
    """Validate that the configuration file exists."""
    resolved_path = config_path.expanduser().resolve()
    if not resolved_path.exists():
        _echo_error(
            "E002",
            f"Configuration file not found: {resolved_path}. Provide a valid path via --config/-c.",
        )
        raise typer.Exit(code=2)


def _validate_output_dir(output_dir: Path) -> None:
    """Validate that the output directory is writable."""
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        _echo_error("E002", f"Cannot create output directory: {output_dir}. {exc}")
        raise typer.Exit(code=2) from exc


def create_pipeline_command(
    pipeline_class: type[PipelineBase],
    command_config: Any,  # CommandConfig from registry
) -> Callable[..., None]:
    """Create a Typer command function for a pipeline."""

    pipeline_module_name = pipeline_class.__module__
    pipeline_class_name = pipeline_class.__name__

    def command(
        config: Path = typer.Option(
            ...,
            "--config",
            "-c",
            help="Path to configuration file",
            exists=False,
        ),
        output_dir: Path = typer.Option(
            ...,
            "--output-dir",
            "-o",
            help="Output directory for pipeline artifacts",
        ),
        dry_run: bool = typer.Option(
            False,
            "--dry-run",
            "-d",
            help="Load, merge, and validate configuration without executing the pipeline",
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
            "-v",
            help="Enable verbose (DEBUG-level) logging output",
        ),
        set_overrides: list[str] = typer.Option(
            [],
            "--set",
            "-S",
            help="Override individual configuration keys at runtime (KEY=VALUE). Repeatable.",
        ),
        sample: int | None = typer.Option(
            None,
            "--sample",
            help="Randomly sample N rows using a deterministic seed",
            min=1,
        ),
        limit: int | None = typer.Option(
            None,
            "--limit",
            help="Process at most N rows (useful for smoke runs)",
            min=1,
        ),
        extended: bool = typer.Option(
            False,
            "--extended",
            help="Enable extended QC artifacts and metrics",
        ),
        fail_on_schema_drift: bool = typer.Option(
            True,
            "--fail-on-schema-drift/--allow-schema-drift",
            help="Fail the run on schema drift (disable to log and continue)",
        ),
        validate_columns: bool = typer.Option(
            True,
            "--validate-columns/--no-validate-columns",
            help="Enforce strict column validation (disable to ignore column drift)",
        ),
        golden: Path | None = typer.Option(
            None,
            "--golden",
            help="Path to golden dataset for bitwise determinism comparison",
            exists=False,
        ),
        input_file: Path | None = typer.Option(
            None,
            "--input-file",
            "-i",
            help="Optional path to input file (CSV/Parquet) containing IDs for batch extraction",
            exists=False,
        ),
    ) -> None:
        """Execute the pipeline command."""
        runner = PipelineCliCommand(
            pipeline_module_name=pipeline_module_name,
            pipeline_class_name=pipeline_class_name,
            command_config=command_config,
        )
        runner.invoke(
            config=config,
            output_dir=output_dir,
            dry_run=dry_run,
            verbose=verbose,
            set_overrides=set_overrides,
            sample=sample,
            limit=limit,
            extended=extended,
            fail_on_schema_drift=fail_on_schema_drift,
            validate_columns=validate_columns,
            golden=golden,
            input_file=input_file,
        )

    # Set command metadata
    command.__doc__ = command_config.description
    return command


def _resolve_pipeline_class(
    *,
    module_name: str,
    class_name: str,
    log: LoggerProtocol,
    run_id: str,
    command_name: str,
) -> type[PipelineBase]:
    """Resolve pipeline class lazily to decouple CLI from pipeline modules."""
    try:
        pipeline_module = importlib.import_module(module_name)
        pipeline_cls = getattr(pipeline_module, class_name)
    except (ModuleNotFoundError, AttributeError) as exc:  # pragma: no cover - defensive guard
        log.error(LogEvents.CLI_PIPELINE_CLASS_LOOKUP_FAILED,
            pipeline=command_name,
            module=module_name,
            class_name=class_name,
            run_id=run_id,
            error=str(exc),
            exc_info=True,
        )
        _echo_error("E002", "Pipeline class could not be resolved. Check installation.")
        raise typer.Exit(code=2) from exc

    if isinstance(pipeline_cls, type):
        if not issubclass(pipeline_cls, PipelineBase):
            log.error(LogEvents.CLI_PIPELINE_CLASS_INVALID,
                pipeline=command_name,
                module=module_name,
                class_name=class_name,
                run_id=run_id,
            )
            _echo_error("E002", "Pipeline type mismatch. Expected PipelineBase subclass.")
            raise typer.Exit(code=2)
    elif not callable(pipeline_cls):
        log.error(LogEvents.CLI_PIPELINE_CLASS_INVALID,
            pipeline=command_name,
            module=module_name,
            class_name=class_name,
            run_id=run_id,
        )
        _echo_error("E002", "Pipeline entry is not callable.")
        raise typer.Exit(code=2)

    return cast(type[PipelineBase], pipeline_cls)


def _handle_pipeline_exception(
    *,
    exc: Exception,
    log: LoggerProtocol,
    run_id: str,
    command_name: str,
) -> None:
    """Map runtime exceptions to deterministic exit codes and logs."""

    if _is_requests_api_error(exc):
        _emit_external_api_failure(
            exc=exc,
            log=log,
            run_id=run_id,
            command_name=command_name,
        )

    if isinstance(exc, (PipelineTimeoutError, PipelineHTTPError, PipelineNetworkError)):
        _emit_external_api_failure(
            exc=exc,
            log=log,
            run_id=run_id,
            command_name=command_name,
        )

    if isinstance(exc, PipelineError):
        log.error(LogEvents.CLI_RUN_ERROR,
            run_id=run_id,
            pipeline=command_name,
            error=str(exc),
            error_type=exc.__class__.__name__,
            cause=str(exc.__cause__) if exc.__cause__ else None,
            exc_info=True,
        )
        _echo_error("E001", f"Пайплайн завершился с ошибкой: {exc}")
        raise typer.Exit(code=1) from exc

    log.error(LogEvents.CLI_RUN_ERROR,
        run_id=run_id,
        pipeline=command_name,
        error=str(exc),
        error_type=exc.__class__.__name__,
        cause=str(exc.__cause__) if exc.__cause__ else None,
        exc_info=True,
    )
    _echo_error("E001", f"Pipeline execution failed: {exc}")
    raise typer.Exit(code=1) from exc


def _echo_error(code: str, message: str) -> None:
    """Emit a deterministic error message."""
    CliCommandBase.emit_error(code, message)


def _emit_external_api_failure(
    *,
    exc: Exception,
    log: LoggerProtocol,
    run_id: str,
    command_name: str,
) -> NoReturn:
    log.error(LogEvents.CLI_PIPELINE_API_ERROR,
        run_id=run_id,
        pipeline=command_name,
        error=str(exc),
        error_type=exc.__class__.__name__,
        cause=str(exc.__cause__) if exc.__cause__ else None,
        exc_info=True,
    )
    _echo_error("E003", f"External API failure: {exc}")
    raise typer.Exit(code=3) from exc


def _is_requests_api_error(exc: Exception) -> bool:
    try:
        from requests import exceptions as requests_exceptions
    except ModuleNotFoundError:  # pragma: no cover - requests is a required runtime dep
        return False
    return isinstance(exc, requests_exceptions.RequestException)
