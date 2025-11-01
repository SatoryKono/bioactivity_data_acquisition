"""Typer command factory for the standalone PubChem enrichment pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.sources.pubchem.pipeline import PubChemPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the PubChem pipeline."""

    return PipelineCommandConfig(
        pipeline_name="pubchem",
        pipeline_factory=lambda: PubChemPipeline,
        default_config=get_config_path("pipelines/pubchem.yaml"),
        default_input=Path("data/input/pubchem_lookup.csv"),
        default_output_dir=Path("data/output/pubchem"),
        description="Standalone PubChem enrichment dataset",
    )


__all__ = ["build_command_config"]
