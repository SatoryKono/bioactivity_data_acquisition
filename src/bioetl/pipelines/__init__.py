"""Public pipeline exports."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

__all__ = (
    "PipelineBase",
    "ActivityPipeline",
    "AssayPipeline",
    "TestItemPipeline",
    "TargetPipeline",
    "DocumentPipeline",
    "GtpIupharPipeline",
)

_PIPELINE_EXPORTS: dict[str, str] = {
    "PipelineBase": "bioetl.pipelines.base",
    "ActivityPipeline": "bioetl.pipelines.activity",
    "AssayPipeline": "bioetl.pipelines.assay",
    "TestItemPipeline": "bioetl.pipelines.testitem",
    "TargetPipeline": "bioetl.pipelines.target",
    "DocumentPipeline": "bioetl.pipelines.document",
    "GtpIupharPipeline": "bioetl.sources.iuphar.pipeline",
}

if TYPE_CHECKING:  # pragma: no cover - imported for type checkers only.
    from bioetl.pipelines.activity import ActivityPipeline
    from bioetl.pipelines.assay import AssayPipeline
    from bioetl.pipelines.base import PipelineBase
    from bioetl.pipelines.document import DocumentPipeline
    from bioetl.sources.iuphar.pipeline import GtpIupharPipeline
    from bioetl.pipelines.target import TargetPipeline
    from bioetl.pipelines.testitem import TestItemPipeline


def __getattr__(name: str) -> Any:
    """Lazily resolve pipeline exports.

    The pipelines import the full ETL stack which in turn depends on optional
    third-party packages. Importing them lazily keeps ``bioetl.pipelines``
    importable even when only a subset of extras is installed while still
    raising the original import error as soon as the symbol is accessed.
    """

    try:
        module_name = _PIPELINE_EXPORTS[name]
    except KeyError as exc:  # pragma: no cover - standard attribute error path.
        raise AttributeError(f"module 'bioetl.pipelines' has no attribute {name!r}") from exc
    module = import_module(module_name)
    return getattr(module, name)


def __dir__() -> list[str]:  # pragma: no cover - trivial helper.
    """Ensure ``dir(bioetl.pipelines)`` exposes public exports."""

    return sorted(set(__all__))

