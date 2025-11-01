"""Typer command factory for the ChEMBL test item pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.pipelines.chembl_testitem import TestItemPipeline


def build_command_config(
    *,
    pipeline_name: str = "chembl_testitem",
    default_input: Path | None = Path("data/input/chembl_testitem.csv"),
    default_output_dir: Path = Path("data/output/chembl_testitem"),
    default_config_path: Path | None = None,
) -> PipelineCommandConfig:
    """Return the CLI command configuration for the test item pipeline."""

    config_path = default_config_path or get_config_path("pipelines/chembl_testitem.yaml")
    return PipelineCommandConfig(
        pipeline_name=pipeline_name,
        pipeline_factory=lambda: TestItemPipeline,
        default_config=config_path,
        default_input=default_input,
        default_output_dir=default_output_dir,
        description="ChEMBL molecules + PubChem",
    )


__all__ = ["build_command_config"]
