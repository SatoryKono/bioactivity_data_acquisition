"""Pipeline orchestration primitives."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from infrastructure.config.models.models import CLIConfig
from infrastructure.io import RunArtifacts, WriteArtifacts, WriteResult
from infrastructure.runtime.lazy_loader import resolve_lazy_attr

if TYPE_CHECKING:  # pragma: no cover - import for typing only
    from .base import PipelineBase, RunResult

PipelineRunOptions = CLIConfig

_LAZY_EXPORTS = {
    "ChemblActivityPipeline": "application.pipelines.specs.chembl.activity.run",
    "ChemblAssayPipeline": "application.pipelines.specs.chembl.assay.run",
    "ChemblDocumentPipeline": "application.pipelines.specs.chembl.document.run",
    "ChemblTargetPipeline": "application.pipelines.specs.chembl.target.run",
    "TestItemChemblPipeline": "application.pipelines.specs.chembl.testitem.run",
}

_ALIAS_EXPORTS = {
    "ActivityPipeline": "ChemblActivityPipeline",
    "AssayPipeline": "ChemblAssayPipeline",
    "DocumentPipeline": "ChemblDocumentPipeline",
    "TargetPipeline": "ChemblTargetPipeline",
    "TestItemPipeline": "TestItemChemblPipeline",
}

_lazy_mapping: dict[str, Any] = dict(_LAZY_EXPORTS.items())
_lazy_mapping.update(
    {
        alias: (_LAZY_EXPORTS[target], target)
        for alias, target in _ALIAS_EXPORTS.items()
    }
)

_lazy_resolver = resolve_lazy_attr(globals(), _lazy_mapping, cache=True)


_BASE_EXPORTS = {
    "PipelineBase": "application.pipelines",
    "RunResult": "application.pipelines",
}


def __getattr__(name: str) -> Any:
    if name in _BASE_EXPORTS:
        module = import_module(_BASE_EXPORTS[name])
        value = getattr(module, name)
        globals()[name] = value
        return value
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
