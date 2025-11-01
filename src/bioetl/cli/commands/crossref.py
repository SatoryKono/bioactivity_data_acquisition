"""CLI command factory for the Crossref pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.sources.crossref.pipeline import CrossrefPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the Crossref pipeline."""

    return PipelineCommandConfig(
        pipeline_name="crossref",
        pipeline_factory=lambda: CrossrefPipeline,
        default_config=get_config_path("pipelines/crossref.yaml"),
        default_input=Path("data/input/document.csv"),
        default_output_dir=Path("data/output/crossref"),
        description="Crossref works enrichment dataset",
    )


__all__ = ["build_command_config"]
