"""Typer command factory for the Guide to Pharmacology target pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.sources.iuphar.pipeline import GtpIupharPipeline


def build_command_config(
    *,
    pipeline_name: str = "gtp_iuphar",
    default_input: Path | None = Path("data/input/iuphar_targets.csv"),
    default_output_dir: Path = Path("data/output/iuphar"),
) -> PipelineCommandConfig:
    """Return the CLI command configuration for the IUPHAR pipeline."""

    return PipelineCommandConfig(
        pipeline_name=pipeline_name,
        pipeline_factory=lambda: GtpIupharPipeline,
        default_config=get_config_path("pipelines/iuphar.yaml"),
        default_input=default_input,
        default_output_dir=default_output_dir,
        description="Guide to Pharmacology targets",
    )


__all__ = ["build_command_config"]
