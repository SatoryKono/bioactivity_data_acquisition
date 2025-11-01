"""Common functions for CLI commands."""

from __future__ import annotations

from pathlib import Path

from bioetl.cli.command import PipelineCommandConfig
from bioetl.config.paths import get_config_path
from bioetl.pipelines.base import PipelineBase


def build_chembl_command_config(
    entity: str,
    pipeline_class: type[PipelineBase],
    description: str,
    *,
    default_input: Path | None = None,
    default_output_dir: Path | None = None,
    default_mode: str | None = None,
    mode_choices: tuple[str, ...] | None = None,
    default_config_path: Path | None = None,
) -> PipelineCommandConfig:
    """Build CLI command configuration for ChEMBL pipelines.
    
    Args:
        entity: Pipeline entity name (e.g., "activity", "assay")
        pipeline_class: Pipeline class to instantiate
        description: Command description
        default_input: Default input file path (auto-generated if None)
        default_output_dir: Default output directory (auto-generated if None)
        default_mode: Default execution mode (optional)
        mode_choices: Available mode choices (optional)
        default_config_path: Default config path (auto-generated if None)
    
    Returns:
        PipelineCommandConfig instance
    """
    pipeline_name = f"chembl_{entity}"
    
    if default_input is None:
        default_input = Path(f"data/input/chembl_{entity}.csv")
    
    if default_output_dir is None:
        default_output_dir = Path(f"data/output/chembl_{entity}")
    
    if default_config_path is None:
        default_config_path = get_config_path(f"pipelines/chembl_{entity}.yaml")
    
    return PipelineCommandConfig(
        pipeline_name=pipeline_name,
        pipeline_factory=lambda: pipeline_class,
        default_config=default_config_path,
        default_input=default_input,
        default_output_dir=default_output_dir,
        default_mode=default_mode or "default",
        mode_choices=mode_choices,
        description=description,
    )


def build_external_source_command_config(
    pipeline_name: str,
    pipeline_class: type[PipelineBase],
    description: str,
    *,
    default_input: Path | None = None,
    default_output_dir: Path | None = None,
    default_config_path: Path | None = None,
) -> PipelineCommandConfig:
    """Build CLI command configuration for external source pipelines.
    
    Args:
        pipeline_name: Pipeline name (e.g., "crossref", "openalex")
        pipeline_class: Pipeline class to instantiate
        description: Command description
        default_input: Default input file path (auto-generated if None)
        default_output_dir: Default output directory (auto-generated if None)
        default_config_path: Default config path (auto-generated if None)
    
    Returns:
        PipelineCommandConfig instance
    """
    if default_input is None:
        default_input = Path(f"data/input/document.csv")
    
    if default_output_dir is None:
        default_output_dir = Path(f"data/output/{pipeline_name}")
    
    if default_config_path is None:
        default_config_path = get_config_path(f"pipelines/{pipeline_name}.yaml")
    
    return PipelineCommandConfig(
        pipeline_name=pipeline_name,
        pipeline_factory=lambda: pipeline_class,
        default_config=default_config_path,
        default_input=default_input,
        default_output_dir=default_output_dir,
        description=description,
    )


__all__ = ["build_chembl_command_config", "build_external_source_command_config"]

