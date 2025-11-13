"""Static command registry for BioETL CLI.

This module defines the static registry of all available pipeline commands.
Adding a new pipeline requires explicitly adding its configuration to this registry.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

from bioetl.pipelines.base import PipelineBase

__all__ = ["CommandConfig", "ToolCommandConfig", "COMMAND_REGISTRY", "TOOL_COMMANDS"]


@dataclass(frozen=True)
class CommandConfig:
    """Configuration for a pipeline command."""

    name: str
    description: str
    pipeline_class: type[Any]
    default_config_path: Path | None = None


@dataclass(frozen=True)
class ToolCommandConfig:
    """Configuration for standalone CLI tools shipped with BioETL."""

    name: str
    description: str
    module: str
    attribute: str = "app"


def build_command_config_activity() -> CommandConfig:
    """Build command configuration for activity pipeline."""
    return _build_config(
        command_name="activity_chembl",
        description=(
            "Extract biological activity records from ChEMBL API and normalize them to the project schema."
        ),
        pipeline_path="bioetl.pipelines.chembl.activity.run.ChemblActivityPipeline",
        default_config="configs/pipelines/activity/activity_chembl.yaml",
    )


def build_command_config_assay() -> CommandConfig:
    """Build command configuration for assay pipeline."""
    return _build_config(
        command_name="assay_chembl",
        description="Extract assay records from ChEMBL API.",
        pipeline_path="bioetl.pipelines.chembl.assay.run.ChemblAssayPipeline",
        default_config="configs/pipelines/assay/assay_chembl.yaml",
    )


def build_command_config_target() -> CommandConfig:
    """Build command configuration for target pipeline."""
    return _build_config(
        command_name="target_chembl",
        description=(
            "Extract target records from ChEMBL API and normalize them to the project schema."
        ),
        pipeline_path="bioetl.pipelines.chembl.target.run.ChemblTargetPipeline",
        default_config="configs/pipelines/target/target_chembl.yaml",
    )


def build_command_config_document() -> CommandConfig:
    """Build command configuration for document pipeline."""
    return _build_config(
        command_name="document_chembl",
        description=(
            "Extract document records from ChEMBL API and normalize them to the project schema."
        ),
        pipeline_path="bioetl.pipelines.chembl.document.run.ChemblDocumentPipeline",
        default_config="configs/pipelines/document/document_chembl.yaml",
    )


def build_command_config_testitem() -> CommandConfig:
    """Build command configuration for testitem pipeline."""
    return _build_config(
        command_name="testitem_chembl",
        description="Extract molecule records from ChEMBL API and normalize them to test items.",
        pipeline_path="bioetl.pipelines.chembl.testitem.run.TestItemChemblPipeline",
        default_config="configs/pipelines/testitem/testitem_chembl.yaml",
    )


def build_command_config_pubchem() -> CommandConfig:
    """Build command configuration for pubchem pipeline."""
    raise NotImplementedError("PubChem pipeline not yet implemented")


def build_command_config_uniprot() -> CommandConfig:
    """Build command configuration for uniprot pipeline."""
    raise NotImplementedError("UniProt pipeline not yet implemented")


def build_command_config_iuphar() -> CommandConfig:
    """Build command configuration for iuphar pipeline."""
    raise NotImplementedError("IUPHAR pipeline not yet implemented")


def build_command_config_openalex() -> CommandConfig:
    """Build command configuration for openalex pipeline."""
    raise NotImplementedError("OpenAlex pipeline not yet implemented")


def build_command_config_crossref() -> CommandConfig:
    """Build command configuration for crossref pipeline."""
    raise NotImplementedError("Crossref pipeline not yet implemented")


def build_command_config_pubmed() -> CommandConfig:
    """Build command configuration for pubmed pipeline."""
    raise NotImplementedError("PubMed pipeline not yet implemented")


def build_command_config_semantic_scholar() -> CommandConfig:
    """Build command configuration for semantic_scholar pipeline."""
    raise NotImplementedError("Semantic Scholar pipeline not yet implemented")


def _build_config(
    *,
    command_name: str,
    description: str,
    pipeline_path: str,
    default_config: str | None,
) -> CommandConfig:
    """Resolve the pipeline class and construct a command configuration."""
    pipeline_class = _load_pipeline_class(pipeline_path)
    default_path = Path(default_config) if default_config is not None else None
    return CommandConfig(
        name=command_name,
        description=description,
        pipeline_class=pipeline_class,
        default_config_path=default_path,
    )


def _load_pipeline_class(path: str) -> type[Any]:
    """Import and return the pipeline class referenced by the dotted path."""
    module_path, class_name = path.rsplit(".", 1)
    module = import_module(module_path)
    pipeline_cls = getattr(module, class_name)
    if not isinstance(pipeline_cls, type):
        msg = f"Object '{class_name}' from '{module_path}' is not a class."
        raise TypeError(msg)
    if not issubclass(pipeline_cls, PipelineBase):
        msg = f"Class '{class_name}' from '{module_path}' is not a PipelineBase subclass."
        raise TypeError(msg)
    return pipeline_cls


# Static registry mapping command names to their build functions
COMMAND_REGISTRY: dict[str, Callable[[], CommandConfig]] = {
    "activity_chembl": build_command_config_activity,
    "assay_chembl": build_command_config_assay,
    "testitem_chembl": build_command_config_testitem,
    "target_chembl": build_command_config_target,
    "document_chembl": build_command_config_document,
    # "pubchem": build_command_config_pubchem,
    # "uniprot": build_command_config_uniprot,
    # "gtp_iuphar": build_command_config_iuphar,
    # "openalex": build_command_config_openalex,
    # "crossref": build_command_config_crossref,
    # "pubmed": build_command_config_pubmed,
    # "semantic_scholar": build_command_config_semantic_scholar,
}


TOOL_COMMANDS: dict[str, ToolCommandConfig] = {
    "qc_boundary_check": ToolCommandConfig(
        name="bioetl-qc-boundary-check",
        description=(
            "Static verification that prevents direct or indirect imports of bioetl.qc from the CLI layer."
        ),
        module="bioetl.cli.tools.qc_boundary_check",
        attribute="main",
    ),
}
