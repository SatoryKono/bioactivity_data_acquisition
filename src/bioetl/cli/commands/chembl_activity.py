"""Typer command factory for the ChEMBL activity pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.pipelines.chembl_activity import ActivityPipeline


def build_command_config(
    *,
    pipeline_name: str = "chembl_activity",
    default_input: Path | None = Path("data/input/chembl_activity.csv"),
    default_output_dir: Path = Path("data/output/chembl_activity"),
    default_config_path: Path | None = None,
) -> PipelineCommandConfig:
    """Return the CLI command configuration for the activity pipeline."""

    config_path = default_config_path or get_config_path("pipelines/chembl_activity.yaml")
    return PipelineCommandConfig(
        pipeline_name=pipeline_name,
        pipeline_factory=lambda: ActivityPipeline,
        default_config=config_path,
        default_input=default_input,
        default_output_dir=default_output_dir,
        description="ChEMBL activity data",
    )


__all__ = ["build_command_config"]
