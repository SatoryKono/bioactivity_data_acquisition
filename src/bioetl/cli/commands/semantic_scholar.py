"""CLI command factory for the Semantic Scholar pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.sources.semantic_scholar.pipeline import SemanticScholarPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the Semantic Scholar pipeline."""

    return PipelineCommandConfig(
        pipeline_name="semantic_scholar",
        pipeline_factory=lambda: SemanticScholarPipeline,
        default_config=get_config_path("pipelines/semantic_scholar.yaml"),
        default_input=Path("data/input/document.csv"),
        default_output_dir=Path("data/output/semantic_scholar"),
        description="Semantic Scholar Graph enrichment dataset",
    )


__all__ = ["build_command_config"]
