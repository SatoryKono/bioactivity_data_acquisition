"""ChEMBL pipeline helpers and run entry points."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_EXPORTS = {
    "activity_run": "bioetl.pipelines.chembl.activity.run",
    "assay_run": "bioetl.pipelines.chembl.assay.run",
    "document_run": "bioetl.pipelines.chembl.document.run",
    "target_run": "bioetl.pipelines.chembl.target.run",
    "testitem_run": "bioetl.pipelines.chembl.testitem.run",
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


