"""Main CLI entry point for BioETL pipelines."""

# pyright: reportMissingImports=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUntypedFunctionDecorator=false

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import typer

from bioetl.config import load_config
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.chembl.activity import ChemblActivityPipeline

app = typer.Typer(
    name="bioetl",
    help="BioETL command-line interface for executing ETL pipelines.",
    add_completion=False,
)


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
            err=True,
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
    """Extract biological activity records from ChEMBL API and normalize them to the project schema."""
    try:
        # Validate inputs
        _validate_config_path(config)
        _validate_output_dir(output_dir)

        # Parse --set overrides
        cli_overrides: dict[str, Any] = {}
        if set_overrides:
            cli_overrides = _parse_set_overrides(set_overrides)

        # Load configuration with automatic base/determinism profiles
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

        # Update CLI config with runtime flags
        pipeline_config.cli.dry_run = dry_run
        if limit is not None:
            pipeline_config.cli.limit = limit
        pipeline_config.cli.extended = extended
        if golden is not None:
            pipeline_config.cli.golden = str(golden)

        # Update materialization root if output_dir is provided
        pipeline_config.materialization.root = str(output_dir)

        # Validate configuration
        if dry_run:
            typer.echo("Configuration validated successfully (dry-run mode)")
            raise typer.Exit(code=0)

        # Generate run_id
        run_id = str(uuid.uuid4())

        # Initialize logger
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

        # Create and run pipeline
        pipeline = ChemblActivityPipeline(config=pipeline_config, run_id=run_id)

        try:
            result = pipeline.run(
                include_correlation=extended,
                include_qc_metrics=extended,
            )

            log.info(
                "cli_activity_completed",
                run_id=run_id,
                dataset=str(result.write_result.dataset),
                duration_ms=sum(result.stage_durations_ms.values()),
            )

            typer.echo(f"Pipeline completed successfully: {result.write_result.dataset}")
            raise typer.Exit(code=0)

        except Exception as exc:
            # Check if it's an API/external error
            # Handle requests exceptions and network errors
            from requests.exceptions import HTTPError, RequestException, Timeout

            if isinstance(exc, (ConnectionError, TimeoutError, RequestException, Timeout, HTTPError)):
                log.error("cli_activity_api_error", run_id=run_id, error=str(exc), exc_info=True)
                typer.echo(f"Error: External API failure: {exc}", err=True)
                raise typer.Exit(code=3) from exc

            # General pipeline error
            log.error("cli_activity_failed", run_id=run_id, error=str(exc), exc_info=True)
            typer.echo(f"Error: Pipeline execution failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc

    except typer.Exit:
        raise
    except Exception as exc:
        typer.echo(f"Unexpected error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def main() -> None:
    """Entry point for CLI application."""
    app()


if __name__ == "__main__":
    main()

