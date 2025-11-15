"""Shared ChEMBL helpers reused across multiple pipelines."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_EXPORTS = {
    "BatchExtractionContext": "bioetl.chembl.common.descriptor",
    "BatchExtractionStats": "bioetl.chembl.common.descriptor",
    "ChemblExtractionContext": "bioetl.chembl.common.descriptor",
    "ChemblExtractionDescriptor": "bioetl.chembl.common.descriptor",
    "ChemblPipelineBase": "bioetl.chembl.common.descriptor",
    "RowMetadataChanges": "bioetl.chembl.common.normalize",
    "add_row_metadata": "bioetl.chembl.common.normalize",
    "normalize_identifiers": "bioetl.chembl.common.normalize",
    "ChemblEnrichmentScenario": "bioetl.chembl.common.enrich",
    "ChemblOptionalStringValueMixin": "bioetl.chembl.common.mixins",
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

