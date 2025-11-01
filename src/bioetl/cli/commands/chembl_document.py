"""Typer command factory for the ChEMBL document pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.sources.chembl.document.pipeline import DocumentPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the document pipeline."""

    return PipelineCommandConfig(
        pipeline_name="document",
        pipeline_factory=lambda: DocumentPipeline,
        default_config=get_config_path("pipelines/document.yaml"),
        default_input=Path("data/input/document.csv"),
        default_output_dir=Path("data/output/documents"),
        mode_choices=("chembl", "all"),
        description="ChEMBL document enrichment",
    )


__all__ = ["build_command_config"]
