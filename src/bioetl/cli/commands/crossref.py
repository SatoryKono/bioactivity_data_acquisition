"""CLI command factory for the Crossref pipeline."""

from __future__ import annotations

from bioetl.cli.command import PipelineCommandConfig
from bioetl.cli.commands._common import build_external_source_command_config
from bioetl.sources.crossref.pipeline import CrossrefPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the Crossref pipeline."""

    return build_external_source_command_config(
        pipeline_name="crossref",
        pipeline_class=CrossrefPipeline,
        description="Crossref works enrichment dataset",
    )


__all__ = ["build_command_config"]
