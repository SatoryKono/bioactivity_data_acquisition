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
from bioetl.sources.pubchem.pipeline import PubChemPipeline
from bioetl.sources.uniprot.pipeline import UniProtPipeline

PIPELINE_FACTORIES: Mapping[str, Type[PipelineBase]] = {
    "chembl_activity": ActivityPipeline,
    "chembl_assay": AssayPipeline,
    "chembl_document": DocumentPipeline,
    "chembl_target": TargetPipeline,
    "chembl_testitem": TestItemPipeline,
    "pubchem_molecule": PubChemPipeline,
    "uniprot_protein": UniProtPipeline,
    "iuphar_target": GtpIupharPipeline,
}


def get_pipeline(key: str) -> Type[PipelineBase]:
    """Return the registered pipeline implementation for ``key``."""

    try:
        return PIPELINE_FACTORIES[key]
    except KeyError as exc:  # pragma: no cover - defensive branch
        raise KeyError(f"Unknown pipeline key: {key}") from exc


def iter_pipelines() -> Mapping[str, Type[PipelineBase]]:
    """Expose a copy of the registry mapping."""

    return dict(PIPELINE_FACTORIES)


__all__ = ["PIPELINE_FACTORIES", "get_pipeline", "iter_pipelines"]
