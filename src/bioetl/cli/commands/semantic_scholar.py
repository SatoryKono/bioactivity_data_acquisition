"""CLI command factory for the Semantic Scholar pipeline."""

from __future__ import annotations

from bioetl.cli.command import PipelineCommandConfig
from bioetl.cli.commands._common import build_external_source_command_config
from bioetl.sources.semantic_scholar.pipeline import SemanticScholarPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the Semantic Scholar pipeline."""

    return build_external_source_command_config(
        pipeline_name="semantic_scholar",
        pipeline_class=SemanticScholarPipeline,
        description="Semantic Scholar Graph enrichment dataset",
    )


__all__ = ["build_command_config"]
