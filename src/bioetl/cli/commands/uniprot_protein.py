"""Typer command factory for the UniProt enrichment pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.cli.commands._common import build_external_source_command_config
from bioetl.sources.uniprot.pipeline import UniProtPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the UniProt pipeline."""

    return build_external_source_command_config(
        pipeline_name="uniprot",
        pipeline_class=UniProtPipeline,
        description="Standalone UniProt enrichment",
        default_input=Path("data/input/uniprot.csv"),
    )


__all__ = ["build_command_config"]
