"""Pipeline orchestration primitives."""

from __future__ import annotations

from typing import Any

from bioetl.config.models.models import CLIConfig
from bioetl.core.io import RunArtifacts, WriteArtifacts, WriteResult
from bioetl.core.runtime.lazy_loader import resolve_lazy_attr

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

_lazy_mapping: dict[str, Any] = {
    name: module for name, module in _LAZY_EXPORTS.items()
}
_lazy_mapping.update(
    {
        alias: (_LAZY_EXPORTS[target], target)
        for alias, target in _ALIAS_EXPORTS.items()
    }
)

_lazy_resolver = resolve_lazy_attr(globals(), _lazy_mapping, cache=True)


def __getattr__(name: str) -> Any:
    return _lazy_resolver(name)


# pyright: ignore[reportUnsupportedDunderAll]
__all__ = [
    "PipelineBase",
    "RunArtifacts",
    "RunResult",
    "WriteArtifacts",
    "WriteResult",
    "PipelineRunOptions",
]

