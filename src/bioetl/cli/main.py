"""Main CLI entry point for BioETL pipelines."""

# pyright: reportMissingImports=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUntypedFunctionDecorator=false

from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import typer

from bioetl.config import load_config
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.chembl.activity import ChemblActivityPipeline

app = typer.Typer(
    name="bioetl",
    help="BioETL command-line interface for executing ETL pipelines.",
    add_completion=False,
)


def _execute_activity(
    *,
    config: Path,
    output_dir: Path,
    dry_run: bool,
    limit: int | None,
    extended: bool,
    set_overrides: list[str] | None,
    golden: Path | None,
    sample: int | None = None,
    verbose: bool = False,
    fail_on_schema_drift: bool = True,
    validate_columns: bool = True,
) -> None:
    """Shared implementation powering both the root command and activity subcommand."""

    if limit is not None and sample is not None:
        raise typer.BadParameter("--limit and --sample are mutually exclusive")

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

    pipeline_config.cli.dry_run = dry_run
    if limit is not None:
        pipeline_config.cli.limit = limit
    if sample is not None:
        pipeline_config.cli.sample = sample
    pipeline_config.cli.extended = extended
    if golden is not None:
        pipeline_config.cli.golden = str(golden)
    pipeline_config.cli.verbose = verbose
    pipeline_config.cli.fail_on_schema_drift = fail_on_schema_drift
    pipeline_config.cli.validate_columns = validate_columns
    if not validate_columns:
        pipeline_config.validation.strict = False

    pipeline_config.materialization.root = str(output_dir)

    # Configure logging prior to pipeline execution
    log_level = "DEBUG" if verbose else "INFO"
    from bioetl.core.logger import LoggerConfig

    UnifiedLogger.configure(LoggerConfig(level=log_level))

    if dry_run:
        typer.echo("Configuration validated successfully (dry-run mode)")
        raise typer.Exit(code=0)

    # Generate run_id and deterministic date tag
    run_id = str(uuid.uuid4())
    timezone_name = pipeline_config.determinism.environment.timezone
    try:
        tz = ZoneInfo(timezone_name)
    except Exception:  # pragma: no cover - invalid timezone handled by defaults
        tz = ZoneInfo("UTC")
    pipeline_config.cli.date_tag = (
        pipeline_config.cli.date_tag or datetime.now(tz).strftime("%Y%m%d")
    )

    log = UnifiedLogger.get(__name__)
    log.info(
        "cli_activity_started",
        config=str(config),
        output_dir=str(output_dir),
        run_id=run_id,
        dry_run=dry_run,
        limit=limit,
        extended=extended,
    )

    pipeline = ChemblActivityPipeline(pipeline_config, run_id)

    try:
        mode = "extended" if extended else None
        include_correlation = extended or pipeline_config.postprocess.correlation.enabled
        include_qc_metrics = extended

        result = pipeline.run(
            mode=mode,
            include_correlation=include_correlation,
            include_qc_metrics=include_qc_metrics,
        )

        log.info(
            "cli_activity_completed",
            run_id=run_id,
            dataset=str(result.write_result.dataset),
            duration_ms=sum(result.stage_durations_ms.values()),
        )

        typer.echo(f"Pipeline completed successfully: {result.write_result.dataset}")
        raise typer.Exit(code=0)

    except typer.Exit:
        raise
    except Exception as exc:
        from requests.exceptions import HTTPError, RequestException, Timeout

        if isinstance(exc, (ConnectionError, TimeoutError, RequestException, Timeout, HTTPError)):
            log.error("cli_activity_api_error", run_id=run_id, error=str(exc), exc_info=True)
            typer.echo(f"Error: External API failure: {exc}", err=True)
            raise typer.Exit(code=3) from exc

        log.error("cli_activity_failed", run_id=run_id, error=str(exc), exc_info=True)
        typer.echo(f"Error: Pipeline execution failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc


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
    # Resolve relative paths relative to current working directory
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


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    config: Path | None = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to configuration file",
        exists=False,
    ),
    output_dir: Path | None = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Output directory for pipeline artifacts",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Load, merge, and validate configuration without executing the pipeline",
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
    set_overrides: list[str] = typer.Option(
        [],
        "--set",
        "-S",
        help="Override individual configuration keys at runtime (KEY=VALUE). Repeatable.",
    ),
    golden: Path | None = typer.Option(
        None,
        "--golden",
        help="Path to golden dataset for bitwise determinism comparison",
        exists=False,
    ),
) -> None:
    """Allow invoking the CLI without explicitly specifying the `activity` subcommand."""

    if ctx.invoked_subcommand is not None:
        return

    if config is None or output_dir is None:
        raise typer.BadParameter("Missing required options --config and --output-dir")

    _execute_activity(
        config=config,
        output_dir=output_dir,
        dry_run=dry_run,
        limit=limit,
        extended=extended,
        set_overrides=set_overrides,
        golden=golden,
    )


@app.command(name="activity")
def activity(

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
        help="Load, merge, and validate configuration without executing the pipeline",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        help="Process at most N rows (useful for smoke runs)",
        min=1,
    ),
    sample: int | None = typer.Option(
        None,
        "--sample",
        help="Randomly sample N rows using a deterministic seed",
        min=1,
    ),
    extended: bool = typer.Option(
        False,
        "--extended",
        help="Enable extended QC artifacts and metrics",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Enable verbose (DEBUG-level) logging output",
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
    set_overrides: list[str] = typer.Option(
        [],
        "--set",
        "-S",
        help="Override individual configuration keys at runtime (KEY=VALUE). Repeatable.",
    ),
    golden: Path | None = typer.Option(
        None,
        "--golden",
        help="Path to golden dataset for bitwise determinism comparison",
        exists=False,
    ),
) -> None:
    """Extract biological activity records from ChEMBL API and normalize them to the project schema."""
    _execute_activity(
        config=config,
        output_dir=output_dir,
        dry_run=dry_run,
        limit=limit,
        sample=sample,
        extended=extended,
        verbose=verbose,
        fail_on_schema_drift=fail_on_schema_drift,
        validate_columns=validate_columns,
        set_overrides=set_overrides,
        golden=golden,
    )


def run() -> None:
    """Entry point for CLI application (alias for main)."""
    app()


if __name__ == "__main__":
    run()

