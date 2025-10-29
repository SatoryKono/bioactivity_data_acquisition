#!/usr/bin/env python3
"""CLI entrypoint for executing the target pipeline."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from types import MethodType

import pandas as pd
import typer

from bioetl.config.loader import load_config, parse_cli_overrides
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.target import TargetPipeline

DEFAULT_CONFIG = Path("configs/pipelines/target.yaml")
DEFAULT_INPUT = Path("data/input/targets.csv")
DEFAULT_OUTPUT_ROOT = Path("data/output")

app = typer.Typer(help="Run target pipeline to extract and transform target data")


def _validate_positive_option(value: int | None, param_name: str) -> None:
    """Ensure CLI numeric options are positive integers when provided."""

    if value is not None and value < 1:
        raise typer.BadParameter(
            f"--{param_name} must be >= 1",
            param_name=param_name,
        )


@app.command()
def run(  # noqa: PLR0913 - CLI functions naturally accept many parameters
    input_file: Path | None = typer.Option(
        DEFAULT_INPUT,
        "--input-file",
        "-i",
        help="Path to the seed dataset used during extraction",
    ),
    output_root: Path = typer.Option(
        DEFAULT_OUTPUT_ROOT,
        "--output-root",
        "-o",
        help="Directory where pipeline run artifacts will be materialised",
    ),
    config_path: Path = typer.Option(
        DEFAULT_CONFIG,
        "--config",
        help="Path to the pipeline configuration YAML",
    ),
    golden: Path | None = typer.Option(
        None,
        "--golden",
        help="Optional golden dataset for deterministic comparisons",
    ),
    sample: int | None = typer.Option(
        None,
        "--sample",
        help="Process only the first N records for smoke testing",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        help="Restrict extraction to the first N input records",
    ),
    fail_on_schema_drift: bool = typer.Option(
        True,
        "--fail-on-schema-drift/--allow-schema-drift",
        help="Fail immediately if schema drift is detected",
        show_default=True,
    ),
    extended: bool = typer.Option(
        False,
        "--extended/--no-extended",
        help="Emit extended QC artifacts (correlations, metadata)",
        show_default=True,
    ),
    mode: str = typer.Option(
        "default",
        "--mode",
        help="Execution mode for the pipeline",
        show_default=True,
    ),
    run_id: str | None = typer.Option(
        None,
        "--run-id",
        help="Explicit run identifier (defaults to pipeline+timestamp)",
    ),
    with_uniprot: bool = typer.Option(
        True,
        "--with-uniprot/--without-uniprot",
        help="Enable UniProt enrichment stage",
        show_default=True,
    ),
    with_iuphar: bool = typer.Option(
        True,
        "--with-iuphar/--without-iuphar",
        help="Enable IUPHAR enrichment stage",
        show_default=True,
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run/--no-dry-run",
        "-d",
        help="Validate configuration without running the pipeline",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose/--no-verbose",
        "-v",
        help="Enable verbose logging",
    ),
    set_values: list[str] = typer.Option(
        [],
        "--set",
        "-S",
        metavar="KEY=VALUE",
        help="Override configuration values (repeatable)",
    ),
) -> None:
    """Execute the target pipeline with run-scoped materialisation."""

    _validate_positive_option(sample, "sample")
    _validate_positive_option(limit, "limit")

    UnifiedLogger.setup(mode="development" if verbose else "production")
    logger = UnifiedLogger.get("cli.target")

    overrides = parse_cli_overrides(set_values)
    cli_overrides = overrides.setdefault("cli", {})
    cli_overrides.update(
        {
            "fail_on_schema_drift": fail_on_schema_drift,
            "extended": extended,
            "mode": mode,
            "dry_run": dry_run,
            "verbose": verbose,
        }
    )
    if golden is not None:
        cli_overrides["golden"] = str(golden)
    if sample is not None:
        cli_overrides["sample"] = sample
    if limit is not None:
        cli_overrides["limit"] = limit

    config = load_config(config_path, overrides=overrides)
    mode_choices: list[str] | None = None
    if isinstance(config.cli, dict):
        mode_choices = config.cli.get("mode_choices")
    if mode_choices and mode not in mode_choices:
        allowed = ", ".join(mode_choices)
        raise typer.BadParameter(
            f"Mode must be one of: {allowed}",
            param_name="mode",
        )

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    resolved_run_id = run_id or f"{config.pipeline.name}_{timestamp}"
    config.cli.setdefault("stages", {})
    config.cli["stages"].update(
        {"uniprot": with_uniprot, "iuphar": with_iuphar}
    )
    config.cli["run_id"] = resolved_run_id
    config.cli["output_root"] = str(output_root)

    logger.info(
        "pipeline_run_configured",
        run_id=resolved_run_id,
        config=str(config_path),
        output_root=str(output_root),
    )

    pipeline = TargetPipeline(config, resolved_run_id)

    runtime_flags: dict[str, Any] = {
        "with_uniprot": with_uniprot,
        "with_iuphar": with_iuphar,
        "dry_run": dry_run,
        "mode": mode,
    }
    if sample is not None:
        runtime_flags["sample"] = sample
    if limit is not None:
        runtime_flags["limit"] = limit
    if input_file is not None:
        runtime_flags["input_file"] = str(input_file)

    pipeline.runtime_options.update(runtime_flags)

    if dry_run:
        typer.echo("[DRY-RUN] Configuration loaded successfully.")
        typer.echo(f"Run ID: {resolved_run_id}")
        typer.echo(f"Config path: {config_path}")
        typer.echo(f"Config hash: {config.config_hash}")
        typer.echo(f"Stages: UniProt={with_uniprot}, IUPHAR={with_iuphar}")
        return

    if sample is not None:
        original_extract = pipeline.extract

        def limited_extract(*args: Any, **kwargs: Any) -> pd.DataFrame:  # type: ignore[misc]
            df = original_extract(*args, **kwargs)
            logger.info(
                "applying_sample_limit",
                limit=sample,
                original_rows=len(df),
            )
            return df.head(sample)

        pipeline.extract = MethodType(limited_extract, pipeline)  # type: ignore[method-assign]

    if input_file is None:
        input_file = DEFAULT_INPUT
        pipeline.runtime_options["input_file"] = str(input_file)

    run_directory = output_root / resolved_run_id / config.pipeline.entity
    dataset_dir = run_directory / "datasets"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    dataset_path = dataset_dir / f"{config.pipeline.entity}.csv"

    artifacts = pipeline.run(
        dataset_path,
        extended=extended,
        input_file=input_file,
    )

    typer.echo("=== Target Pipeline Execution Summary ===")
    typer.echo(f"Run directory: {artifacts.run_directory}")
    typer.echo(f"Dataset: {artifacts.dataset}")
    typer.echo(f"Quality report: {artifacts.quality_report}")
    if artifacts.metadata is not None:
        typer.echo(f"Metadata: {artifacts.metadata}")
    if artifacts.qc_summary:
        typer.echo(f"QC summary: {artifacts.qc_summary}")
    if artifacts.qc_missing_mappings:
        typer.echo(f"QC missing mappings: {artifacts.qc_missing_mappings}")
    if artifacts.qc_enrichment_metrics:
        typer.echo(f"QC enrichment metrics: {artifacts.qc_enrichment_metrics}")
    if artifacts.additional_datasets:
        typer.echo("Additional datasets:")
        for name, path in artifacts.additional_datasets.items():
            typer.echo(f"  - {name}: {path}")

    logger.info(
        "pipeline_run_completed",
        run_id=resolved_run_id,
        dataset=str(artifacts.dataset),
        quality_report=str(artifacts.quality_report),
        metadata=str(artifacts.metadata) if artifacts.metadata else None,
    )


if __name__ == "__main__":
    app()
