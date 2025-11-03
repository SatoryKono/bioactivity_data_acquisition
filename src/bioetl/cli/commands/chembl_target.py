"""Typer command factory for the ChEMBL target pipeline."""

from __future__ import annotations

from bioetl.cli.command import PipelineCommandConfig
from bioetl.cli.commands._common import build_chembl_command_config
from bioetl.pipelines.chembl_target import TargetPipeline


def build_command_config(**kwargs) -> PipelineCommandConfig:
    """Return the CLI command configuration for the target pipeline."""
    return build_chembl_command_config(
        entity="target",
        pipeline_class=TargetPipeline,
        description="ChEMBL + UniProt + IUPHAR",
        mode_choices=("default", "smoke"),
        **kwargs,
    )


__all__ = ["build_command_config"]
