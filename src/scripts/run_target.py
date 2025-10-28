#!/usr/bin/env python3
"""Script for running target pipeline."""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import typer

from bioetl.config.loader import load_config
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.target import TargetPipeline

app = typer.Typer(help="Run target pipeline to extract and transform target data")


@app.command()
def main(
    input_file: Path = typer.Option(
        Path("data/input/target.csv"),
        "--input-file",
        "-i",
        help="Path to input CSV file",
    ),
    profile: str = typer.Option(
        "dev",
        "--profile",
        "-p",
        help="Configuration profile (dev/prod/test)",
    ),
    output_dir: Path = typer.Option(
        Path("data/output/target"),
        "--output-dir",
        "-o",
        help="Directory for output files",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-d",
        help="Dry run without writing files",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Verbose logging",
    ),
    limit: int = typer.Option(
        None,
        "--limit",
        "-l",
        help="Limit number of rows to process",
    ),
) -> None:
    """Run target pipeline with specified parameters."""
    # Setup logging
    log_mode = "development" if verbose else "production"
    UnifiedLogger.setup(mode=log_mode)
    logger = UnifiedLogger.get(__name__)

    try:

        logger.info("script_started", profile=profile, dry_run=dry_run, limit=limit)

        # Load configuration
        profile_path = Path(f"configs/profiles/{profile}.yaml")
        if not profile_path.exists():
            logger.error("profile_not_found", path=profile_path)
            typer.echo(f"Profile not found: {profile_path}", err=True)
            sys.exit(1)

        pipeline_config_path = Path("configs/pipelines/target.yaml")
        if not pipeline_config_path.exists():
            logger.error("pipeline_config_not_found", path=pipeline_config_path)
            typer.echo(f"Pipeline config not found: {pipeline_config_path}", err=True)
            sys.exit(1)

        # Load configurations
        logger.info("loading_config", profile=profile_path)
        profile_config = load_config(profile_path)
        pipeline_config = load_config(pipeline_config_path)

        # Merge configurations
        # Pipeline config should override profile config
        merged_config = profile_config.model_copy(deep=True)
        merged_config.pipeline = pipeline_config.pipeline
        merged_config.sources.update(pipeline_config.sources)
        merged_config.determinism = pipeline_config.determinism
        merged_config.qc = pipeline_config.qc

        logger.info("config_loaded", pipeline=merged_config.pipeline.name)

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d")
        output_filename = f"target_{timestamp}.csv"
        output_path = output_dir / output_filename

        logger.info("output_path", path=output_path)

        # Create pipeline instance
        run_id = f"target_{timestamp}"
        pipeline = TargetPipeline(merged_config, run_id)

        # Run pipeline
        if dry_run:
            logger.info("dry_run_mode")
            typer.echo("Running in DRY-RUN mode (no files will be written)")

            # Extract
            typer.echo("Extracting data...")
            df = pipeline.extract(input_file)
            typer.echo(f"[OK] Extracted {len(df)} rows")

            # Apply limit if specified
            if limit and limit > 0:
                df = df.head(limit)
                typer.echo(f"[OK] Limited to {len(df)} rows")

            # Transform
            typer.echo("Transforming data...")
            df = pipeline.transform(df)
            typer.echo(f"[OK] Transformed {len(df)} rows")

            # Validate (skip export)
            typer.echo("Validating data...")
            df = pipeline.validate(df)
            typer.echo(f"[OK] Validated {len(df)} rows")

            # Show summary
            typer.echo("\n=== Dry Run Summary ===")
            typer.echo(f"Rows processed: {len(df)}")
            typer.echo(f"Columns: {len(df.columns)}")
            typer.echo(f"Output path: {output_path}")
            typer.echo("\nSample data:")
            typer.echo(df.head(3).to_string())

        else:
            logger.info("pipeline_execution_start")
            typer.echo("Starting pipeline execution...")

            # Apply limit by modifying extract behavior temporarily
            if limit and limit > 0:
                original_extract = pipeline.extract

                def limited_extract(input_file: Path | None = None) -> pd.DataFrame:
                    """Extract with optional row limit."""
                    df = original_extract(input_file)
                    logger.info("applying_row_limit", limit=limit, original_rows=len(df))
                    df = df.head(limit)
                    return df

                pipeline.extract = limited_extract

            # Run full pipeline
            artifacts = pipeline.run(output_path)

            # Summary
            typer.echo("\n=== Pipeline Execution Summary ===")
            typer.echo(f"[OK] Dataset: {artifacts.dataset}")
            typer.echo(f"[OK] Quality report: {artifacts.quality_report}")
            if artifacts.correlation_report:
                typer.echo(f"[OK] Correlation report: {artifacts.correlation_report}")
            if artifacts.metadata:
                typer.echo(f"[OK] Metadata: {artifacts.metadata}")

            logger.info("script_completed", output_path=artifacts.dataset)

        typer.echo("\n[OK] Pipeline completed successfully!")
        sys.exit(0)

    except Exception as e:
        logger.error("pipeline_failed", error=str(e), exc_info=True)
        typer.echo(f"\n[ERROR] Pipeline failed: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    app()

