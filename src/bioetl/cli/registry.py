"""Central registry of pipeline CLI command configurations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from bioetl.cli.command import PipelineCommandConfig
from bioetl.cli.commands.chembl_activity import build_command_config as build_activity_command
from bioetl.cli.commands.chembl_assay import build_command_config as build_assay_command
from bioetl.cli.commands.chembl_document import build_command_config as build_document_command
from bioetl.cli.commands.chembl_target import build_command_config as build_target_command
from bioetl.cli.commands.chembl_testitem import build_command_config as build_testitem_command
from bioetl.cli.commands.iuphar_target import build_command_config as build_iuphar_command
from bioetl.cli.commands.crossref import build_command_config as build_crossref_command
from bioetl.cli.commands.openalex import build_command_config as build_openalex_command
from bioetl.cli.commands.pubmed import build_command_config as build_pubmed_command
from bioetl.cli.commands.semantic_scholar import (
    build_command_config as build_semantic_scholar_command,
)
from bioetl.cli.commands.pubchem_molecule import build_command_config as build_pubchem_command
from bioetl.cli.commands.uniprot_protein import build_command_config as build_uniprot_command


_LEGACY_OVERRIDES = {
    "chembl_activity": "activity",
    "chembl_assay": "assay",
    "chembl_document": "document",
    "chembl_target": "target",
    "chembl_testitem": "testitem",
    "pubchem_molecule": "pubchem",
    "iuphar_target": "gtp_iuphar",
    "uniprot_protein": "uniprot",
}


def _resolve_legacy_key(key: str) -> str | None:
    if key in _LEGACY_OVERRIDES:
        return _LEGACY_OVERRIDES[key]
    if "_" in key:
        _, suffix = key.split("_", 1)
        return suffix
    return None


def build_registry() -> dict[str, PipelineCommandConfig]:
    """Construct the default CLI registry mapping names to command configs."""

    return {
        "activity": build_activity_command(),
        "assay": build_assay_command(),
        "document": build_document_command(),
        "target": build_target_command(),
        "testitem": build_testitem_command(),
        "pubchem": build_pubchem_command(),
        "gtp_iuphar": build_iuphar_command(),
        "uniprot": build_uniprot_command(),
        "openalex": build_openalex_command(),
        "crossref": build_crossref_command(),
        "pubmed": build_pubmed_command(),
        "semantic_scholar": build_semantic_scholar_command(),
    }


def get_command_config(key: str) -> PipelineCommandConfig:
    """Return a defensive copy of the pipeline CLI configuration for ``key``."""

    registry = build_registry()
    try:
        config = registry[key]
    except KeyError as exc:  # pragma: no cover - defensive branch
        legacy_key = _resolve_legacy_key(key)
        if legacy_key and legacy_key in registry:
            config = registry[legacy_key]
        else:
            raise KeyError(f"Unknown pipeline key: {key}") from exc
    return replace(config)


def iter_commands() -> Mapping[str, PipelineCommandConfig]:
    """Yield the default command registry without allowing direct mutation."""

    return build_registry().copy()


__all__ = ["build_registry", "get_command_config", "iter_commands"]
