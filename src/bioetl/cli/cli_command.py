"""Common command options and pipeline command factory for BioETL CLI."""
from __future__ import annotations

import importlib
from collections.abc import Callable
from pathlib import Path
from typing import Any, NoReturn, cast

import typer
from structlog.stdlib import BoundLogger

from bioetl.config.environment import load_environment_settings as _load_environment_settings
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import (
    CLI_ERROR_CONFIG,
    CLI_ERROR_EXTERNAL_API,
    CLI_ERROR_INTERNAL,
)
from bioetl.core.pipeline import PipelineBase
from bioetl.core.pipeline.errors import (
    PipelineError,
    PipelineHTTPError,
    PipelineNetworkError,
    PipelineTimeoutError,
)
from bioetl.core.runtime.cli_pipeline_runner import (
    ConfigLoadError,
    EnvironmentSetupError,
    PipelineCommandOptions,
    PipelineCommandRunner,
    PipelineConfigFactory,
    PipelineDryRunPlan,
    PipelineExecutionPlan,
    coerce_option_value,
    parse_set_overrides,
    validate_config_path,
    validate_output_dir,
)

load_environment_settings = _load_environment_settings

__all__ = ["create_pipeline_command", "CommonOptions", "load_environment_settings"]


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
        """Capture shared pipeline CLI options in a normalized container."""
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
    """Typer command implementation responsible for running a pipeline."""

    def __init__(
        self,
        *,
        pipeline_module_name: str,
        pipeline_class_name: str,
        command_config: Any,
        runner: PipelineCommandRunner | None = None,
    ) -> None:
        """Initialize the pipeline command runner with metadata and logger."""
        super().__init__(logger=UnifiedLogger.get(__name__))
        self._pipeline_module_name = pipeline_module_name
        self._pipeline_class_name = pipeline_class_name
        self._command_config = command_config
        self._run_id: str | None = None
        self._runner = runner

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
        """Execute the pipeline workflow with normalized options."""
        dry_run = cast(bool, coerce_option_value(dry_run))
        verbose = cast(bool, coerce_option_value(verbose))
        override_values = cast(list[str], coerce_option_value(set_overrides, default=[]))
        sample = cast(int | None, coerce_option_value(sample))
        limit = cast(int | None, coerce_option_value(limit))
        extended = cast(bool, coerce_option_value(extended))
        fail_on_schema_drift = cast(bool, coerce_option_value(fail_on_schema_drift))
        validate_columns = cast(bool, coerce_option_value(validate_columns))
        golden = cast(Path | None, coerce_option_value(golden))
        input_file = cast(Path | None, coerce_option_value(input_file))

        if limit is not None and sample is not None:
            raise typer.BadParameter("--limit and --sample are mutually exclusive")

        try:
            config = validate_config_path(config)
        except FileNotFoundError as exc:
            CliCommandBase.emit_error(
                template=CLI_ERROR_CONFIG,
                message=str(exc),
                event=LogEvents.CONFIG_MISSING,
                context={"config_path": str(config)},
                exit_code=2,
                cause=exc,
            )

        try:
            output_dir = validate_output_dir(output_dir)
        except OSError as exc:
            CliCommandBase.emit_error(
                template=CLI_ERROR_CONFIG,
                message=str(exc),
                event=LogEvents.CONFIG_INVALID,
                context={"output_dir": str(output_dir)},
                exit_code=2,
                cause=exc,
            )

        try:
            cli_overrides = parse_set_overrides(override_values) if override_values else {}
        except ValueError as exc:
            raise typer.BadParameter(str(exc))

        options = PipelineCommandOptions(
            config_path=config,
            output_dir=output_dir,
            dry_run=dry_run,
            verbose=verbose,
            set_overrides=cli_overrides,
            sample=sample,
            limit=limit,
            extended=extended,
            fail_on_schema_drift=fail_on_schema_drift,
            validate_columns=validate_columns,
            golden=golden,
            input_file=input_file,
        )

        runner = self._get_runner()
        try:
            plan = runner.prepare(options)
        except EnvironmentSetupError as exc:
            self.emit_error(
                template=CLI_ERROR_CONFIG,
                message=f"Environment validation failed: {exc}",
                logger=self.logger,
                event=LogEvents.CLI_RUN_ERROR,
                context={"pipeline": self._command_config.name},
                exit_code=2,
                cause=exc,
            )
            return
        except ConfigLoadError as exc:
            if exc.missing_reference:
                message = f"Configuration file or referenced profile not found: {exc}"
            else:
                message = f"Configuration validation failed: {exc}"
            self.emit_error(
                template=CLI_ERROR_CONFIG,
                message=message,
                logger=self.logger,
                event=LogEvents.CONFIG_INVALID,
                context={"pipeline": self._command_config.name},
                exit_code=2,
                cause=exc,
            )
            return

        if isinstance(plan, PipelineDryRunPlan):
            typer.echo("Configuration validated successfully (dry-run mode)")
            self.exit(0)

        if not isinstance(plan, PipelineExecutionPlan):  # pragma: no cover - defensive guard
            self.emit_error(
                template=CLI_ERROR_INTERNAL,
                message="Invalid pipeline execution plan generated",
                logger=self.logger,
                event=LogEvents.CLI_RUN_ERROR,
                context={"pipeline": self._command_config.name},
                exit_code=self.exit_code_error,
            )
            return

        run_id = plan.run_id
        self._run_id = run_id

        log = self.logger
        pipeline_factory = _resolve_pipeline_class(
            module_name=self._pipeline_module_name,
            class_name=self._pipeline_class_name,
            log=log,
            run_id=run_id,
            command_name=self._command_config.name,
        )

        try:
            result = runner.execute_plan(
                plan,
                pipeline_factory=pipeline_factory,
                logger=self.logger,
                command_name=self._command_config.name,
                config_path=config,
                output_dir=output_dir,
            )
        except typer.Exit:
            raise
        except Exception as exc:  # noqa: BLE001
            self.handle_exception(exc)
            return

        typer.echo(f"Pipeline completed successfully: {result.write_result.dataset}")
        self.exit(0)

    def handle_exception(self, exc: Exception) -> NoReturn:
        """Handle pipeline exceptions, log context, and exit with an error code."""
        run_id = self._run_id or "unknown"
        _handle_pipeline_exception(
            exc=exc,
            log=self.logger,
            run_id=run_id,
            command_name=self._command_config.name,
        )
        self.exit(self.exit_code_error)
        raise AssertionError("unreachable exit path")

    def _get_runner(self) -> PipelineCommandRunner:
        if self._runner is not None:
            return self._runner
        config_factory = PipelineConfigFactory(
            environment_loader=load_environment_settings,
        )
        return PipelineCommandRunner(config_factory=config_factory)

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
    log: BoundLogger,
    run_id: str,
    command_name: str,
) -> type[PipelineBase]:
    """Resolve pipeline class lazily to decouple CLI from pipeline modules."""
    try:
        pipeline_module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:  # pragma: no cover - defensive guard
        CliCommandBase.emit_error(
            template=CLI_ERROR_CONFIG,
            message="Pipeline module could not be resolved. Check installation.",
            logger=log,
            event=LogEvents.CLI_PIPELINE_CLASS_LOOKUP_FAILED,
            context={
                "pipeline": command_name,
                "module": module_name,
                "class_name": class_name,
                "run_id": run_id,
                "exc_info": True,
            },
            exit_code=2,
            cause=exc,
        )
        raise

    try:
        pipeline_cls = getattr(pipeline_module, class_name)
    except AttributeError as exc:  # pragma: no cover - defensive guard
        CliCommandBase.emit_error(
            template=CLI_ERROR_CONFIG,
            message="Pipeline class could not be resolved. Check installation.",
            logger=log,
            event=LogEvents.CLI_PIPELINE_CLASS_LOOKUP_FAILED,
            context={
                "pipeline": command_name,
                "module": module_name,
                "class_name": class_name,
                "run_id": run_id,
                "exc_info": True,
            },
            exit_code=2,
            cause=exc,
        )
        raise

    if isinstance(pipeline_cls, type):
        if not issubclass(pipeline_cls, PipelineBase):
            CliCommandBase.emit_error(
                template=CLI_ERROR_CONFIG,
                message="Pipeline type mismatch. Expected PipelineBase subclass.",
                logger=log,
                event=LogEvents.CLI_PIPELINE_CLASS_INVALID,
                context={"pipeline": command_name, "module": module_name, "class_name": class_name},
                exit_code=2,
            )
    elif not callable(pipeline_cls):
        CliCommandBase.emit_error(
            template=CLI_ERROR_CONFIG,
            message="Pipeline entry is not callable.",
            logger=log,
            event=LogEvents.CLI_PIPELINE_CLASS_INVALID,
            context={"pipeline": command_name, "module": module_name, "class_name": class_name},
            exit_code=2,
        )

    return cast(type[PipelineBase], pipeline_cls)


def _handle_pipeline_exception(
    *,
    exc: Exception,
    log: BoundLogger,
    run_id: str,
    command_name: str,
) -> None:
    """Map runtime exceptions to deterministic exit codes and logs."""

    context: dict[str, Any] = {
        "run_id": run_id,
        "pipeline": command_name,
        "error_type": exc.__class__.__name__,
        "cause": str(exc.__cause__) if exc.__cause__ else None,
        "exc_info": True,
    }
    if _is_requests_api_error(exc):
        _emit_external_api_failure(
            exc=exc,
            log=log,
            context=context,
        )

    if isinstance(exc, (PipelineTimeoutError, PipelineHTTPError, PipelineNetworkError)):
        _emit_external_api_failure(
            exc=exc,
            log=log,
            context=context,
        )

    if isinstance(exc, PipelineError):
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Pipeline finished with error: {exc}",
            logger=log,
            event=LogEvents.CLI_RUN_ERROR,
            context=context,
            exit_code=1,
            cause=exc,
        )

    CliCommandBase.emit_error(
        template=CLI_ERROR_INTERNAL,
        message=f"Pipeline execution failed: {exc}",
        logger=log,
        event=LogEvents.CLI_RUN_ERROR,
        context=context,
        exit_code=1,
        cause=exc,
    )


def _emit_external_api_failure(
    *,
    exc: Exception,
    log: BoundLogger,
    context: dict[str, Any],
) -> NoReturn:
    """Log external API failure context and exit with the dedicated error code."""
    CliCommandBase.emit_error(
        template=CLI_ERROR_EXTERNAL_API,
        message=f"External API failure: {exc}",
        logger=log,
        event=LogEvents.CLI_PIPELINE_API_ERROR,
        context={**context, "error_message": str(exc)},
        exit_code=3,
        cause=exc,
    )


def _is_requests_api_error(exc: Exception) -> bool:
    """Return True when the exception originates from requests API errors."""
    try:
        from requests import exceptions as requests_exceptions
    except ModuleNotFoundError:  # pragma: no cover - requests is a required runtime dep
        return False
    return isinstance(exc, requests_exceptions.RequestException)
