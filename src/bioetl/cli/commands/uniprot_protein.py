"""Typer command factory for the UniProt enrichment pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.sources.uniprot.pipeline import UniProtPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the UniProt pipeline."""

    return PipelineCommandConfig(
        pipeline_name="uniprot",
        pipeline_factory=lambda: UniProtPipeline,
        default_config=get_config_path("pipelines/uniprot.yaml"),
        default_input=Path("data/input/uniprot.csv"),
        default_output_dir=Path("data/output/uniprot"),
        description="Standalone UniProt enrichment",
    )


__all__ = ["build_command_config"]
