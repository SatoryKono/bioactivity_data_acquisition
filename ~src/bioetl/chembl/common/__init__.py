"""Shared ChEMBL helpers reused across multiple pipelines."""
from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "BatchExtractionContext",
    "BatchExtractionStats",
    "ChemblExtractionContext",
    "ChemblExtractionDescriptor",
    "ChemblPipelineBase",
    "ChemblEnrichmentScenario",
    "ChemblOptionalStringValueMixin",
    "ChemblHandshakeResult",
    "ChemblReleaseMixin",
    "perform_chembl_handshake",
    "RowMetadataChanges",
    "add_row_metadata",
    "normalize_identifiers",
    "join_activity_with_molecule",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    "BatchExtractionContext": ("bioetl.chembl.common.descriptor", "BatchExtractionContext"),
    "BatchExtractionStats": ("bioetl.chembl.common.descriptor", "BatchExtractionStats"),
    "ChemblExtractionContext": ("bioetl.chembl.common.descriptor", "ChemblExtractionContext"),
    "ChemblExtractionDescriptor": ("bioetl.chembl.common.descriptor", "ChemblExtractionDescriptor"),
    "ChemblPipelineBase": ("bioetl.chembl.common.descriptor", "ChemblPipelineBase"),
    "ChemblEnrichmentScenario": ("bioetl.chembl.common.enrich", "ChemblEnrichmentScenario"),
    "ChemblOptionalStringValueMixin": ("bioetl.chembl.common.mixins", "ChemblOptionalStringValueMixin"),
    "ChemblHandshakeResult": ("bioetl.chembl.common.release_tracker", "ChemblHandshakeResult"),
    "ChemblReleaseMixin": ("bioetl.chembl.common.release_tracker", "ChemblReleaseMixin"),
    "perform_chembl_handshake": ("bioetl.chembl.common.release_tracker", "perform_chembl_handshake"),
    "RowMetadataChanges": ("bioetl.chembl.common.normalize", "RowMetadataChanges"),
    "add_row_metadata": ("bioetl.chembl.common.normalize", "add_row_metadata"),
    "normalize_identifiers": ("bioetl.chembl.common.normalize", "normalize_identifiers"),
    "join_activity_with_molecule": ("bioetl.chembl.common.molecule_join", "join_activity_with_molecule"),
}


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise AttributeError(name) from exc

    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
