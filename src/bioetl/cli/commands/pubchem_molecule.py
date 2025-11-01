"""Typer command factory for the standalone PubChem enrichment pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.cli.commands._common import build_external_source_command_config
from bioetl.sources.pubchem.pipeline import PubChemPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the PubChem pipeline."""

    return build_external_source_command_config(
        pipeline_name="pubchem",
        pipeline_class=PubChemPipeline,
        description="Standalone PubChem enrichment dataset",
        default_input=Path("data/input/pubchem_lookup.csv"),
    )


__all__ = ["build_command_config"]
