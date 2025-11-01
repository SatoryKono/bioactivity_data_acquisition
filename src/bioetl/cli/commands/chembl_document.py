"""Typer command factory for the ChEMBL document pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.sources.chembl.document.pipeline import DocumentPipeline


def build_command_config(
    *,
    pipeline_name: str = "document",
    default_input: Path | None = Path("data/input/document.csv"),
    default_output_dir: Path = Path("data/output/documents"),
    default_mode: str = "chembl",
) -> PipelineCommandConfig:
    """Return the CLI command configuration for the document pipeline."""

    return PipelineCommandConfig(
        pipeline_name=pipeline_name,
        pipeline_factory=lambda: DocumentPipeline,
        default_config=get_config_path("pipelines/document.yaml"),
        default_input=default_input,
        default_output_dir=default_output_dir,
        default_mode=default_mode,
        mode_choices=("chembl", "all"),
        description="ChEMBL document enrichment",
    )


__all__ = ["build_command_config"]
