"""Canonical registry of pipeline classes keyed by source/object pairs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Type

from bioetl.pipelines.base import PipelineBase
from bioetl.pipelines.chembl_activity import ActivityPipeline
from bioetl.pipelines.chembl_assay import AssayPipeline
from bioetl.sources.chembl.document.pipeline import DocumentPipeline
from bioetl.sources.chembl.target.pipeline import TargetPipeline
from bioetl.sources.chembl.testitem.pipeline import TestItemPipeline
from bioetl.sources.iuphar.pipeline import GtpIupharPipeline
from bioetl.sources.crossref.pipeline import CrossrefPipeline
from bioetl.sources.openalex.pipeline import OpenAlexPipeline
from bioetl.sources.pubchem.pipeline import PubChemPipeline
from bioetl.sources.pubmed.pipeline import PubMedPipeline
from bioetl.sources.semantic_scholar.pipeline import SemanticScholarPipeline
from bioetl.sources.uniprot.pipeline import UniProtPipeline

PIPELINE_REGISTRY: Mapping[str, Type[PipelineBase]] = {
    "chembl_activity": ActivityPipeline,
    "chembl_assay": AssayPipeline,
    "chembl_document": DocumentPipeline,
    "chembl_target": TargetPipeline,
    "chembl_testitem": TestItemPipeline,
    "pubchem_molecule": PubChemPipeline,
    "uniprot_protein": UniProtPipeline,
    "gtp_iuphar": GtpIupharPipeline,
    "openalex": OpenAlexPipeline,
    "crossref": CrossrefPipeline,
    "pubmed": PubMedPipeline,
    "semantic_scholar": SemanticScholarPipeline,
}


def get_pipeline(key: str) -> Type[PipelineBase]:
    """Return the registered pipeline implementation for ``key``."""

    try:
        return PIPELINE_REGISTRY[key]
    except KeyError as exc:  # pragma: no cover - defensive branch
        raise KeyError(f"Unknown pipeline key: {key}") from exc


def iter_pipelines() -> Mapping[str, Type[PipelineBase]]:
    """Expose a copy of the registry mapping."""

    return dict(PIPELINE_REGISTRY)


__all__ = ["PIPELINE_REGISTRY", "get_pipeline", "iter_pipelines"]
