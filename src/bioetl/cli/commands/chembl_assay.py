"""Typer command factory for the ChEMBL assay pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.pipelines.chembl.chembl_assay import AssayPipeline


def build_command_config(
    *,
    pipeline_name: str = "assay",
    default_input: Path | None = Path("data/input/assay.csv"),
    default_output_dir: Path = Path("data/output/assay"),
) -> PipelineCommandConfig:
    """Return the CLI command configuration for the assay pipeline."""

    return PipelineCommandConfig(
        pipeline_name=pipeline_name,
        pipeline_factory=lambda: AssayPipeline,
        default_config=get_config_path("pipelines/chembl/assay.yaml"),
        default_input=default_input,
        default_output_dir=default_output_dir,
        description="ChEMBL assay data",
    )


__all__ = ["build_command_config"]
