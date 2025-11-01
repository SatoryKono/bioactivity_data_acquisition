"""Typer command factory for the ChEMBL target pipeline."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.pipelines.chembl_target import TargetPipeline


def build_command_config(
    *,
    pipeline_name: str = "chembl_target",
    default_input: Path | None = Path("data/input/chembl_target.csv"),
    default_output_dir: Path = Path("data/output/chembl_target"),
    default_config_path: Path | None = None,
) -> PipelineCommandConfig:
    """Return the CLI command configuration for the target pipeline."""

    config_path = default_config_path or get_config_path("pipelines/chembl_target.yaml")
    return PipelineCommandConfig(
        pipeline_name=pipeline_name,
        pipeline_factory=lambda: TargetPipeline,
        default_config=config_path,
        default_input=default_input,
        default_output_dir=default_output_dir,
        mode_choices=("default", "smoke"),
        description="ChEMBL + UniProt + IUPHAR",
    )


__all__ = ["build_command_config"]
