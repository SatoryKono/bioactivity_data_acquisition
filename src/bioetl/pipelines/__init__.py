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
    "UniProtPipeline",
    "OpenAlexPipeline",
    "CrossrefPipeline",
    "PubMedPipeline",
    "SemanticScholarPipeline",
)

_PIPELINE_EXPORTS: dict[str, str] = {
    "PipelineBase": "bioetl.pipelines.base",
    "ActivityPipeline": "bioetl.sources.chembl.activity.pipeline",
    "AssayPipeline": "bioetl.sources.chembl.assay.pipeline",
    "TestItemPipeline": "bioetl.sources.chembl.testitem.pipeline",
    "TargetPipeline": "bioetl.sources.chembl.target.pipeline",
    "DocumentPipeline": "bioetl.sources.chembl.document.pipeline",
    "GtpIupharPipeline": "bioetl.sources.iuphar.pipeline",
    "UniProtPipeline": "bioetl.sources.uniprot.pipeline",
    "OpenAlexPipeline": "bioetl.sources.openalex.pipeline",
    "CrossrefPipeline": "bioetl.sources.crossref.pipeline",
    "PubMedPipeline": "bioetl.sources.pubmed.pipeline",
    "SemanticScholarPipeline": "bioetl.sources.semantic_scholar.pipeline",
}

if TYPE_CHECKING:  # pragma: no cover - imported for type checkers only.
    from bioetl.pipelines.base import PipelineBase
    from bioetl.sources.chembl.activity.pipeline import ActivityPipeline
    from bioetl.sources.chembl.assay.pipeline import AssayPipeline
    from bioetl.sources.chembl.document.pipeline import DocumentPipeline
    from bioetl.sources.chembl.target.pipeline import TargetPipeline
    from bioetl.sources.chembl.testitem.pipeline import TestItemPipeline
    from bioetl.sources.iuphar.pipeline import GtpIupharPipeline
    from bioetl.sources.openalex.pipeline import OpenAlexPipeline
    from bioetl.sources.crossref.pipeline import CrossrefPipeline
    from bioetl.sources.uniprot.pipeline import UniProtPipeline
    from bioetl.sources.pubmed.pipeline import PubMedPipeline
    from bioetl.sources.semantic_scholar.pipeline import SemanticScholarPipeline


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
