"""Static command registry for BioETL CLI.

This module defines the static registry of all available pipeline commands.
Adding a new pipeline requires explicitly adding its configuration to this registry.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = ["CommandConfig", "COMMAND_REGISTRY"]


@dataclass(frozen=True)
class CommandConfig:
    """Configuration for a pipeline command."""

    name: str
    description: str
    pipeline_class: type[Any]
    default_config_path: Path | None = None


def build_command_config_activity() -> CommandConfig:
    """Build command configuration for activity pipeline."""
    from bioetl.pipelines.activity.activity import ChemblActivityPipeline

    return CommandConfig(
        name="activity_chembl",
        description="Extract biological activity records from ChEMBL API and normalize them to the project schema.",
        pipeline_class=ChemblActivityPipeline,
        default_config_path=Path("configs/pipelines/activity/activity_chembl.yaml"),
    )


def build_command_config_assay() -> CommandConfig:
    """Build command configuration for assay pipeline."""
    from bioetl.pipelines.assay.assay import ChemblAssayPipeline

    return CommandConfig(
        name="assay_chembl",
        description="Extract assay records from ChEMBL API.",
        pipeline_class=ChemblAssayPipeline,
        default_config_path=Path("configs/pipelines/assay/assay_chembl.yaml"),
    )


def build_command_config_target() -> CommandConfig:
    """Build command configuration for target pipeline."""
    from bioetl.pipelines.target.target import ChemblTargetPipeline

    return CommandConfig(
        name="target",
        description="Extract target records from ChEMBL API and normalize them to the project schema.",
        pipeline_class=ChemblTargetPipeline,
        default_config_path=Path("configs/pipelines/target/target_chembl.yaml"),
    )


def build_command_config_document() -> CommandConfig:
    """Build command configuration for document pipeline."""
    from bioetl.pipelines.document.document import ChemblDocumentPipeline

    return CommandConfig(
        name="document",
        description="Extract document records from ChEMBL API and normalize them to the project schema.",
        pipeline_class=ChemblDocumentPipeline,
        default_config_path=Path("configs/pipelines/document/document_chembl.yaml"),
    )


def build_command_config_testitem() -> CommandConfig:
    """Build command configuration for testitem pipeline."""
    from bioetl.pipelines.testitem.testitem import TestItemChemblPipeline

    return CommandConfig(
        name="testitem_chembl",
        description="Extract molecule records from ChEMBL API and normalize them to test items.",
        pipeline_class=TestItemChemblPipeline,
        default_config_path=Path("configs/pipelines/testitem/testitem_chembl.yaml"),
    )


def build_command_config_pubchem() -> CommandConfig:
    """Build command configuration for pubchem pipeline."""
    # TODO: Import when pubchem pipeline is implemented
    raise NotImplementedError("PubChem pipeline not yet implemented")


def build_command_config_uniprot() -> CommandConfig:
    """Build command configuration for uniprot pipeline."""
    # TODO: Import when uniprot pipeline is implemented
    raise NotImplementedError("UniProt pipeline not yet implemented")


def build_command_config_iuphar() -> CommandConfig:
    """Build command configuration for iuphar pipeline."""
    # TODO: Import when iuphar pipeline is implemented
    raise NotImplementedError("IUPHAR pipeline not yet implemented")


def build_command_config_openalex() -> CommandConfig:
    """Build command configuration for openalex pipeline."""
    # TODO: Import when openalex pipeline is implemented
    raise NotImplementedError("OpenAlex pipeline not yet implemented")


def build_command_config_crossref() -> CommandConfig:
    """Build command configuration for crossref pipeline."""
    # TODO: Import when crossref pipeline is implemented
    raise NotImplementedError("Crossref pipeline not yet implemented")


def build_command_config_pubmed() -> CommandConfig:
    """Build command configuration for pubmed pipeline."""
    # TODO: Import when pubmed pipeline is implemented
    raise NotImplementedError("PubMed pipeline not yet implemented")


def build_command_config_semantic_scholar() -> CommandConfig:
    """Build command configuration for semantic_scholar pipeline."""
    # TODO: Import when semantic_scholar pipeline is implemented
    raise NotImplementedError("Semantic Scholar pipeline not yet implemented")


# Static registry mapping command names to their build functions
COMMAND_REGISTRY: dict[str, Callable[[], CommandConfig]] = {
    "activity_chembl": build_command_config_activity,
    "assay_chembl": build_command_config_assay,
    "testitem_chembl": build_command_config_testitem,  # Alias
    "target": build_command_config_target,
    "target_chembl": build_command_config_target,  # Alias
    "document_chembl": build_command_config_document,  # Alias
    # "pubchem": build_command_config_pubchem,
    # "uniprot": build_command_config_uniprot,
    # "gtp_iuphar": build_command_config_iuphar,
    # "openalex": build_command_config_openalex,
    # "crossref": build_command_config_crossref,
    # "pubmed": build_command_config_pubmed,
    # "semantic_scholar": build_command_config_semantic_scholar,
}

