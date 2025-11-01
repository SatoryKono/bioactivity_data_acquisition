"""Typer command factory for the ChEMBL activity pipeline."""

from __future__ import annotations

from bioetl.cli.command import PipelineCommandConfig
from bioetl.cli.commands._common import build_chembl_command_config
from bioetl.pipelines.chembl_activity import ActivityPipeline


def build_command_config(**kwargs) -> PipelineCommandConfig:
    """Return the CLI command configuration for the activity pipeline."""
    return build_chembl_command_config(
        entity="activity",
        pipeline_class=ActivityPipeline,
        description="ChEMBL activity data",
        **kwargs,
    )


__all__ = ["build_command_config"]
