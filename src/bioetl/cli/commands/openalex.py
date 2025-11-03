"""CLI command factory for the OpenAlex pipeline."""

from __future__ import annotations

from bioetl.cli.command import PipelineCommandConfig
from bioetl.cli.commands._common import build_external_source_command_config
from bioetl.sources.openalex.pipeline import OpenAlexPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the OpenAlex pipeline."""

    return build_external_source_command_config(
        pipeline_name="openalex",
        pipeline_class=OpenAlexPipeline,
        description="OpenAlex Works enrichment dataset",
    )


__all__ = ["build_command_config"]
