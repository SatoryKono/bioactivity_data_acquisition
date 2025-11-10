"""Common command options and pipeline command factory for BioETL CLI."""

from __future__ import annotations

import importlib
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import typer

from bioetl.config.environment import apply_runtime_overrides, read_environment_settings
from bioetl.config.models.models import CLIConfig, PipelineConfig
from bioetl.config.models.policies import DeterminismConfig, DeterminismEnvironmentConfig
from bioetl.core.logger import LoggerConfig, UnifiedLogger
from bioetl.pipelines.base import PipelineBase

__all__ = ["create_pipeline_command", "CommonOptions", "read_pipeline_config"]

if TYPE_CHECKING:
    def read_pipeline_config_impl(
        *,
        config_path: str | Path,
        profiles: Sequence[str | Path] | None = None,
        cli_overrides: Mapping[str, Any] | None = None,
        env: Mapping[str, str] | None = None,
        env_prefixes: Sequence[str] = ("BIOETL__", "BIOACTIVITY__"),
        include_default_profiles: bool = False,
    ) -> PipelineConfig:
        ...

else:
    from bioetl.config.loader import read_pipeline_config as read_pipeline_config_impl


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
        typer.echo(
            f"Error: Configuration file not found: {resolved_path}\n"
            "Please provide a valid path using --config or -c flag.",
            err=False,
        )
        raise typer.Exit(code=2)


def _validate_output_dir(output_dir: Path) -> None:
    """Validate that the output directory is writable."""
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        typer.echo(
            f"Error: Cannot create output directory: {output_dir}\n{exc}",
            err=True,
        )
        raise typer.Exit(code=2) from exc


def _update_pipeline_config_from_cli(
    pipeline_config: PipelineConfig,
    *,
    dry_run: bool,
    limit: int | None,
    sample: int | None,
    extended: bool,
    golden: Path | None,
    input_file: Path | None,
    verbose: bool,
    fail_on_schema_drift: bool,
    validate_columns: bool,
    output_dir: Path,
) -> None:
    """Apply CLI flag values to the mutable pipeline configuration."""

    cli_config: CLIConfig = pipeline_config.cli

    cli_config.dry_run = dry_run
    if limit is not None:
        cli_config.limit = limit
    if sample is not None:
        cli_config.sample = sample

    cli_config.extended = extended
    if golden is not None:
        cli_config.golden = str(golden)
    if input_file is not None:
        cli_config.input_file = str(input_file)

    cli_config.verbose = verbose
    cli_config.fail_on_schema_drift = fail_on_schema_drift
    cli_config.validate_columns = validate_columns
    if not validate_columns:
        pipeline_config.validation.strict = False

    pipeline_config.materialization.root = str(output_dir)

# ``bioetl.cli.command.read_pipeline_config`` historically served as the public
# entry point for loading pipeline configuration from the CLI module. Preserve
# the same reference for backwards compatibility while delegating to the shared
# loader implementation.
read_pipeline_config = read_pipeline_config_impl


def create_pipeline_command(
    pipeline_class: type[PipelineBase],
    command_config: Any,  # CommandConfig from registry
) -> Callable[..., None]:
    """Create a Typer command function for a pipeline.

    This factory function creates a command function that:
    1. Loads and validates configuration
    2. Instantiates the pipeline
    3. Executes the pipeline run
    4. Handles errors and exit codes

    Parameters
    ----------
    pipeline_class:
        The pipeline class to instantiate.
    command_config:
        The command configuration from the registry.

    Returns
    -------
    Callable:
        A Typer command function ready to be registered.
    """

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
        if limit is not None and sample is not None:
            raise typer.BadParameter("--limit and --sample are mutually exclusive")

        try:
            env_settings = read_environment_settings()
        except ValueError as exc:
            typer.echo(f"Error: Environment validation failed: {exc}", err=True)
            raise typer.Exit(code=2) from exc

        apply_runtime_overrides(env_settings)

        _validate_config_path(config)
        _validate_output_dir(output_dir)

        cli_overrides: dict[str, Any] = {}
        if set_overrides:
            cli_overrides = _parse_set_overrides(set_overrides)

        try:
            pipeline_config = read_pipeline_config(
                config_path=config,
                cli_overrides=cli_overrides,
                include_default_profiles=True,
            )
        except FileNotFoundError as exc:
            typer.echo(
                f"Error: Configuration file or referenced profile not found: {exc}",
                err=True,
            )
            raise typer.Exit(code=2) from exc
        except ValueError as exc:
            typer.echo(
                f"Error: Configuration validation failed: {exc}",
                err=True,
            )
            raise typer.Exit(code=2) from exc

        _update_pipeline_config_from_cli(
            pipeline_config,
            dry_run=dry_run,
            limit=limit,
            sample=sample,
            extended=extended,
            golden=golden,
            input_file=input_file,
            verbose=verbose,
            fail_on_schema_drift=fail_on_schema_drift,
            validate_columns=validate_columns,
            output_dir=output_dir,
        )

        # Configure logging
        log_level = "DEBUG" if verbose else "INFO"
        UnifiedLogger.configure(LoggerConfig(level=log_level))

        if dry_run:
            typer.echo("Configuration validated successfully (dry-run mode)")
            raise typer.Exit(code=0)

        # Generate run_id and deterministic date tag
        run_id = str(uuid.uuid4())
        determinism_config: DeterminismConfig = pipeline_config.determinism
        environment_config: DeterminismEnvironmentConfig = determinism_config.environment
        timezone_name: str = environment_config.timezone
        try:
            tz = ZoneInfo(timezone_name)
        except Exception:  # pragma: no cover - invalid timezone handled by defaults
            tz = ZoneInfo("UTC")
        cli_config: CLIConfig = pipeline_config.cli
        cli_config.date_tag = cli_config.date_tag or datetime.now(tz).strftime("%Y%m%d")

        log = UnifiedLogger.get(__name__)
        log.info(
            "cli_pipeline_started",
            pipeline=command_config.name,
            config=str(config),
            output_dir=str(output_dir),
            run_id=run_id,
            dry_run=dry_run,
            limit=limit,
            extended=extended,
        )

        try:
            pipeline_module = importlib.import_module(pipeline_module_name)
            pipeline_cls = getattr(pipeline_module, pipeline_class_name)
        except (ModuleNotFoundError, AttributeError) as exc:  # pragma: no cover - defensive guard
            log.error(
                "cli_pipeline_class_lookup_failed",
                pipeline=command_config.name,
                module=pipeline_module_name,
                class_name=pipeline_class_name,
                run_id=run_id,
                error=str(exc),
                exc_info=True,
            )
            typer.echo(
                "Error: Pipeline class could not be resolved. Please check installation.",
                err=True,
            )
            raise typer.Exit(code=2) from exc

        pipeline = pipeline_cls(pipeline_config, run_id)

        postprocess_config = pipeline_config.postprocess
        correlation_section = postprocess_config.correlation
        correlation_enabled = bool(correlation_section.enabled)
        effective_extended = bool(extended or pipeline_config.cli.extended)
        run_kwargs: dict[str, Any] = {
            "extended": effective_extended,
            "include_correlation": effective_extended or correlation_enabled,
            "include_qc_metrics": effective_extended,
        }

        try:
            # Use output_dir as output_path for run()
            result = pipeline.run(
                Path(output_dir),
                **run_kwargs,
            )

            log.info(
                "cli_pipeline_completed",
                run_id=run_id,
                pipeline=command_config.name,
                dataset=str(result.write_result.dataset),
                duration_ms=sum(result.stage_durations_ms.values()),
            )

            typer.echo(f"Pipeline completed successfully: {result.write_result.dataset}")
            raise typer.Exit(code=0)

        except typer.Exit:
            raise
        except Exception as exc:
            from requests.exceptions import HTTPError, RequestException, Timeout

            if isinstance(
                exc, (ConnectionError, TimeoutError, RequestException, Timeout, HTTPError)
            ):
                log.error("cli_pipeline_api_error", run_id=run_id, error=str(exc), exc_info=True)
                typer.echo(f"Error: External API failure: {exc}", err=True)
                raise typer.Exit(code=3) from exc

            log.error("cli_pipeline_failed", run_id=run_id, error=str(exc), exc_info=True)
            typer.echo(f"Error: Pipeline execution failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc

    # Set command metadata
    command.__doc__ = command_config.description
    return command
