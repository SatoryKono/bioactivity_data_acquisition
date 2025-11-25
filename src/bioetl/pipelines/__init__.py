from __future__ import annotations

from collections.abc import Callable
from typing import Mapping

from bioetl.pipelines.activity_chembl import ActivityChemblPipeline
from bioetl.pipelines.assays_chembl import AssaysChemblPipeline
from bioetl.pipelines.documents_chembl import DocumentsChemblPipeline
from bioetl.pipelines.targets_chembl import TargetsChemblPipeline
from bioetl.pipelines.testitems_chembl import TestItemsChemblPipeline

PipelineFactory = Callable[[str, Mapping[str, object], bool], object]


def _factory(cls) -> PipelineFactory:
    def _create(run_id: str, config: Mapping[str, object], strict_validation: bool) -> object:
        return cls(run_id, config=config, strict_validation=strict_validation)

    return _create


PIPELINE_REGISTRY: dict[str, PipelineFactory] = {
    "activity_chembl": _factory(ActivityChemblPipeline),
    "assays_chembl": _factory(AssaysChemblPipeline),
    "testitems_chembl": _factory(TestItemsChemblPipeline),
    "targets_chembl": _factory(TargetsChemblPipeline),
    "documents_chembl": _factory(DocumentsChemblPipeline),
}

__all__ = ["PIPELINE_REGISTRY"]
