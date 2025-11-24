"""Shared ChEMBL helpers reused across multiple pipelines."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_EXPORTS = {
    "BatchExtractionContext": "infrastructure.chembl.descriptor",
    "BatchExtractionStats": "infrastructure.chembl.descriptor",
    "ChemblExtractionContext": "infrastructure.chembl.descriptor",
    "ChemblExtractionDescriptor": "infrastructure.chembl.descriptor",
    "ChemblPipelineBase": "infrastructure.chembl.descriptor",
    "RowMetadataChanges": "infrastructure.chembl.normalize",
    "add_row_metadata": "infrastructure.chembl.normalize",
    "normalize_identifiers": "infrastructure.chembl.normalize",
    "ChemblEnrichmentScenario": "infrastructure.chembl.enrich",
    "ChemblOptionalStringValueMixin": "infrastructure.chembl.mixins",
    "ChemblHandshakeResult": "infrastructure.chembl.release_tracker",
    "ChemblReleaseMixin": "infrastructure.chembl.release_tracker",
    "perform_chembl_handshake": "infrastructure.chembl.release_tracker",
    "join_activity_with_molecule": "infrastructure.chembl.molecule_join",
}


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value


__all__ = list(_LAZY_EXPORTS)
