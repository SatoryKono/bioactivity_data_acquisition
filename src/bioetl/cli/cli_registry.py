from __future__ import annotations

from collections.abc import Callable
from typing import Any

from bioetl.config import PipelineConfig
from bioetl.core.pipeline.types import PipelineBaseProtocol

PipelineFactory = Callable[[PipelineConfig, str | None], PipelineBaseProtocol]

PIPELINE_REGISTRY: dict[str, PipelineFactory] = {}


def register_pipeline(name: str, factory: PipelineFactory) -> None:
    if not name:
        raise ValueError("pipeline name must be provided")
    PIPELINE_REGISTRY[name] = factory


def _wrap_pipeline_factory(pipeline_cls: type[Any]) -> PipelineFactory:
    def factory(config: PipelineConfig, run_id: str | None = None) -> object:
        payload = config.model_dump() if hasattr(config, "model_dump") else dict(config)
        return pipeline_cls(payload, run_id=run_id)

    return factory


def _register_default_pipelines() -> None:
    try:
        from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline
        from bioetl.pipelines.chembl.assay.run import ChemblAssayPipeline
        from bioetl.pipelines.chembl.document.run import ChemblDocumentPipeline
        from bioetl.pipelines.chembl.target.run import ChemblTargetPipeline
        from bioetl.pipelines.chembl.thin import (
            ChemblActivityThinPipeline,
            ChemblAssayThinPipeline,
            ChemblDocumentThinPipeline,
            ChemblTargetThinPipeline,
            ChemblTestItemThinPipeline,
        )
    except Exception:  # pragma: no cover - optional dependency loading
        return

    register_pipeline("activity_chembl", _wrap_pipeline_factory(ChemblActivityPipeline))
    register_pipeline("assay_chembl", _wrap_pipeline_factory(ChemblAssayPipeline))
    register_pipeline("document_chembl", _wrap_pipeline_factory(ChemblDocumentPipeline))
    register_pipeline("target_chembl", _wrap_pipeline_factory(ChemblTargetPipeline))
    register_pipeline("activity_chembl_thin", _wrap_pipeline_factory(ChemblActivityThinPipeline))
    register_pipeline("assay_chembl_thin", _wrap_pipeline_factory(ChemblAssayThinPipeline))
    register_pipeline("document_chembl_thin", _wrap_pipeline_factory(ChemblDocumentThinPipeline))
    register_pipeline("target_chembl_thin", _wrap_pipeline_factory(ChemblTargetThinPipeline))
    register_pipeline("testitem_chembl_thin", _wrap_pipeline_factory(ChemblTestItemThinPipeline))


_register_default_pipelines()


__all__ = ["PIPELINE_REGISTRY", "register_pipeline", "PipelineFactory"]
