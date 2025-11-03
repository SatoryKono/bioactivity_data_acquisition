"""CLI command factory for the PubMed pipeline."""

from __future__ import annotations

from bioetl.cli.command import PipelineCommandConfig
from bioetl.cli.commands._common import build_external_source_command_config
from bioetl.sources.pubmed.pipeline import PubMedPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the PubMed pipeline."""

    return build_external_source_command_config(
        pipeline_name="pubmed",
        pipeline_class=PubMedPipeline,
        description="PubMed literature enrichment dataset",
    )


__all__ = ["build_command_config"]
