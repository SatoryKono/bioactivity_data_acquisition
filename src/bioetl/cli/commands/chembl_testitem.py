"""Typer command factory for the ChEMBL test item pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.sources.chembl.testitem.pipeline import TestItemPipeline


def build_command_config() -> PipelineCommandConfig:
    """Return the CLI command configuration for the test item pipeline."""

    return PipelineCommandConfig(
        pipeline_name="testitem",
        pipeline_factory=lambda: TestItemPipeline,
        default_config=get_config_path("pipelines/testitem.yaml"),
        default_input=Path("data/input/testitem.csv"),
        default_output_dir=Path("data/output/testitems"),
        description="ChEMBL molecules + PubChem",
    )


__all__ = ["build_command_config"]
