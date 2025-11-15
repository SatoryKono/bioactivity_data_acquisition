"""Shared ChEMBL pipeline helpers."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_EXPORTS = {
    "BatchExtractionContext": "bioetl.pipelines.chembl.common.descriptor",
    "BatchExtractionStats": "bioetl.pipelines.chembl.common.descriptor",
    "ChemblExtractionContext": "bioetl.pipelines.chembl.common.descriptor",
    "ChemblExtractionDescriptor": "bioetl.pipelines.chembl.common.descriptor",
    "ChemblPipelineBase": "bioetl.pipelines.chembl.common.descriptor",
    "RowMetadataChanges": "bioetl.pipelines.chembl.common.normalize",
    "add_row_metadata": "bioetl.pipelines.chembl.common.normalize",
    "normalize_identifiers": "bioetl.pipelines.chembl.common.normalize",
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

