"""Typer command factory for the ChEMBL document pipeline."""

from __future__ import annotations

from bioetl.cli.command import PipelineCommandConfig
from bioetl.cli.commands._common import build_chembl_command_config
from bioetl.pipelines.chembl_document import DocumentPipeline


def build_command_config(
    *,
    default_mode: str = "all",
    **kwargs
) -> PipelineCommandConfig:
    """Return the CLI command configuration for the document pipeline."""
    return build_chembl_command_config(
        entity="document",
        pipeline_class=DocumentPipeline,
        description="ChEMBL document enrichment",
        default_mode=default_mode,
        mode_choices=("chembl", "all"),
        **kwargs,
    )


__all__ = ["build_command_config"]
