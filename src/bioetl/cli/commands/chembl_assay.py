"""Typer command factory for the ChEMBL assay pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.pipelines.chembl_assay import AssayPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the assay pipeline."""

    return PipelineCommandConfig(
        pipeline_name="assay",
        pipeline_factory=lambda: AssayPipeline,
        default_config=get_config_path("pipelines/assay.yaml"),
        default_input=Path("data/input/assay.csv"),
        default_output_dir=Path("data/output/assay"),
        description="ChEMBL assay data",
    )


__all__ = ["build_command_config"]
