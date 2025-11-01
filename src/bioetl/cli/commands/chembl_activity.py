"""Typer command factory for the ChEMBL activity pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.pipelines.chembl_activity import ActivityPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the activity pipeline."""

    return PipelineCommandConfig(
        pipeline_name="activity",
        pipeline_factory=lambda: ActivityPipeline,
        default_config=get_config_path("pipelines/activity.yaml"),
        default_input=Path("data/input/activity.csv"),
        default_output_dir=Path("data/output/activity"),
        description="ChEMBL activity data",
    )


__all__ = ["build_command_config"]
