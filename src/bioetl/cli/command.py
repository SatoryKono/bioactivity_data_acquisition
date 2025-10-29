"""Shared helpers for pipeline CLI entrypoints."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import typer

from bioetl.config.loader import load_config, parse_cli_overrides
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase


@dataclass(frozen=True)
class PipelineCommandConfig:
    """Configuration describing how to build a CLI command for a pipeline."""

    pipeline_name: str
    pipeline_factory: Callable[[], type[PipelineBase]]
    default_config: Path
    default_input: Path | None
    default_output_dir: Path
    mode_choices: Sequence[str] | None = None
    default_mode: str = "default"


def _validate_sample(sample: int | None) -> None:
    """Raise a user-facing error when sample is invalid."""

    if sample is not None and sample < 1:
        raise typer.BadParameter("--sample must be >= 1", param_name="sample")


def _validate_mode(mode: str, choices: Sequence[str] | None) -> None:
    """Ensure the requested mode is supported by the pipeline."""

    if not choices:
        return

    if mode not in choices:
        allowed = ", ".join(choices)
        raise typer.BadParameter(
            f"Mode must be one of: {allowed}",
            param_name="mode",
        )


def create_pipeline_command(config: PipelineCommandConfig) -> Callable[..., None]:
    """Return a Typer-compatible command function for executing a pipeline."""

    def command(
        input_file: Path | None = typer.Option(
            config.default_input,
            "--input-file",
            "-i",
            help="Path to the seed dataset used during extraction",
        ),
        output_dir: Path = typer.Option(
            config.default_output_dir,
            "--output-dir",
            "-o",
            help="Directory where pipeline outputs will be materialised",
        ),
        config_path: Path = typer.Option(
            config.default_config,
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
            config.default_mode,
            "--mode",
            help="Execution mode for the pipeline",
            show_default=True,
        ),
        dry_run: bool = typer.Option(
            False,
            "--dry-run",
            "-d",
            help="Validate configuration without running the pipeline",
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
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
        """Execute the configured pipeline."""

        _validate_sample(sample)
        _validate_mode(mode, config.mode_choices)

        UnifiedLogger.setup(mode="development" if verbose else "production")
        logger = UnifiedLogger.get(f"cli.{config.pipeline_name}")

        try:
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

            config_obj = load_config(config_path, overrides=overrides)

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            run_id = f"{config.pipeline_name}_{timestamp}"
            pipeline_cls = config.pipeline_factory()
            pipeline = pipeline_cls(config_obj, run_id)

            if dry_run:
                typer.echo("[DRY-RUN] Configuration loaded successfully.")
                typer.echo(f"Config path: {config_path}")
                typer.echo(f"Config hash: {config_obj.config_hash}")
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

                pipeline.extract = limited_extract  # type: ignore[assignment]

            output_dir.mkdir(parents=True, exist_ok=True)
            dataset_name = f"{config.pipeline_name}_{timestamp}.csv"
            output_path = output_dir / dataset_name

            artifacts = pipeline.run(output_path, extended=extended, input_file=input_file)

            typer.echo("=== Pipeline Execution Summary ===")
            typer.echo(f"Dataset: {artifacts.dataset}")
            typer.echo(f"Quality report: {artifacts.quality_report}")
            if artifacts.correlation_report:
                typer.echo(f"Correlation report: {artifacts.correlation_report}")
            if artifacts.metadata:
                typer.echo(f"Metadata: {artifacts.metadata}")
            if artifacts.qc_summary:
                typer.echo(f"QC summary: {artifacts.qc_summary}")
            if artifacts.qc_missing_mappings:
                typer.echo(
                    f"QC missing mappings: {artifacts.qc_missing_mappings}"
                )
            if artifacts.qc_enrichment_metrics:
                typer.echo(
                    f"QC enrichment metrics: {artifacts.qc_enrichment_metrics}"
                )

        except typer.BadParameter:
            raise
        except Exception as exc:  # noqa: BLE001 - surface pipeline errors to CLI
            logger.error("pipeline_failed", error=str(exc))
            typer.secho(f"[ERROR] Pipeline failed: {exc}", fg=typer.colors.RED, err=True)
            raise typer.Exit(1) from exc

    return command

