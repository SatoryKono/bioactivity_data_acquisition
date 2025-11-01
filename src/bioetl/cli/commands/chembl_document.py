"""Typer command factory for the ChEMBL document pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.pipelines.chembl_document import DocumentPipeline


def build_command_config(
    *,
    pipeline_name: str = "chembl_document",
    default_input: Path | None = Path("data/input/chembl_document.csv"),
    default_output_dir: Path = Path("data/output/chembl_document"),
    default_mode: str = "all",
    default_config_path: Path | None = None,
) -> PipelineCommandConfig:
    """Return the CLI command configuration for the document pipeline."""

    config_path = default_config_path or get_config_path("pipelines/chembl_document.yaml")
    return PipelineCommandConfig(
        pipeline_name=pipeline_name,
        pipeline_factory=lambda: DocumentPipeline,
        default_config=config_path,
        default_input=default_input,
        default_output_dir=default_output_dir,
        default_mode=default_mode,
        mode_choices=("chembl", "all"),
        description="ChEMBL document enrichment",
    )


__all__ = ["build_command_config"]
