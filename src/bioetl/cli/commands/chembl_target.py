"""Typer command factory for the ChEMBL target pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.sources.chembl.target.pipeline import TargetPipeline


def build_command_config(
    *,
    pipeline_name: str = "target",
    default_input: Path | None = Path("data/input/target.csv"),
    default_output_dir: Path = Path("data/output/target"),
) -> PipelineCommandConfig:
    """Return the CLI command configuration for the target pipeline."""

    return PipelineCommandConfig(
        pipeline_name=pipeline_name,
        pipeline_factory=lambda: TargetPipeline,
        default_config=get_config_path("pipelines/target.yaml"),
        default_input=default_input,
        default_output_dir=default_output_dir,
        mode_choices=("default", "smoke"),
        description="ChEMBL + UniProt + IUPHAR",
    )


__all__ = ["build_command_config"]
