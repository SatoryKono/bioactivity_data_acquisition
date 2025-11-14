"""Pipeline orchestration primitives."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from bioetl.config.models.models import CLIConfig
from bioetl.core.io import RunArtifacts, WriteArtifacts, WriteResult

from .base import PipelineBase, RunResult

PipelineRunOptions = CLIConfig

_LAZY_EXPORTS = {
    "ChemblActivityPipeline": "bioetl.pipelines.chembl.activity.run",
    "ChemblAssayPipeline": "bioetl.pipelines.chembl.assay.run",
    "ChemblDocumentPipeline": "bioetl.pipelines.chembl.document.run",
    "ChemblTargetPipeline": "bioetl.pipelines.chembl.target.run",
    "TestItemChemblPipeline": "bioetl.pipelines.chembl.testitem.run",
}

_ALIAS_EXPORTS = {
    "ActivityPipeline": "ChemblActivityPipeline",
    "AssayPipeline": "ChemblAssayPipeline",
    "DocumentPipeline": "ChemblDocumentPipeline",
    "TargetPipeline": "ChemblTargetPipeline",
    "TestItemPipeline": "TestItemChemblPipeline",
}


def __getattr__(name: str) -> Any:
    if name in _ALIAS_EXPORTS:
        target = _ALIAS_EXPORTS[name]
        value = getattr(__import__(__name__, fromlist=[target]), target)
        globals()[name] = value
        return value
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value


# pyright: ignore[reportUnsupportedDunderAll]
__all__ = [
    "PipelineBase",
    "RunArtifacts",
    "RunResult",
    "WriteArtifacts",
    "WriteResult",
    "PipelineRunOptions",
]

