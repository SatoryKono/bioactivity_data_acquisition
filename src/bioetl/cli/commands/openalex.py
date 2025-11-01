"""CLI command factory for the OpenAlex pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.sources.openalex.pipeline import OpenAlexPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the OpenAlex pipeline."""

    return PipelineCommandConfig(
        pipeline_name="openalex",
        pipeline_factory=lambda: OpenAlexPipeline,
        default_config=get_config_path("pipelines/openalex.yaml"),
        default_input=Path("data/input/document.csv"),
        default_output_dir=Path("data/output/openalex"),
        description="OpenAlex Works enrichment dataset",
    )


__all__ = ["build_command_config"]
