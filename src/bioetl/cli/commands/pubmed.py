"""CLI command factory for the PubMed pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.sources.pubmed.pipeline import PubMedPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the PubMed pipeline."""

    return PipelineCommandConfig(
        pipeline_name="pubmed",
        pipeline_factory=lambda: PubMedPipeline,
        default_config=get_config_path("pipelines/pubmed.yaml"),
        default_input=Path("data/input/document.csv"),
        default_output_dir=Path("data/output/pubmed"),
        description="PubMed literature enrichment dataset",
    )


__all__ = ["build_command_config"]
