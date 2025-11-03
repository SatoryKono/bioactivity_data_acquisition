"""Typer command factory for the ChEMBL test item pipeline."""

from __future__ import annotations

from bioetl.cli.command import PipelineCommandConfig
from bioetl.cli.commands._common import build_chembl_command_config
from bioetl.pipelines.chembl_testitem import TestItemPipeline


def build_command_config(**kwargs) -> PipelineCommandConfig:
    """Return the CLI command configuration for the test item pipeline."""
    return build_chembl_command_config(
        entity="testitem",
        pipeline_class=TestItemPipeline,
        description="ChEMBL molecules + PubChem",
        **kwargs,
    )


__all__ = ["build_command_config"]
