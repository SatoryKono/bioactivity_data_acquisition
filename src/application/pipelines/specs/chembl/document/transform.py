"""Transform stage wrapper for the ChEMBL document pipeline."""

from __future__ import annotations

from application.pipelines.specs.chembl.stage_runner import build_stage_functions

from .run import ChemblDocumentPipeline

__all__ = ["transform"]

PIPELINE, _STAGES = build_stage_functions(
    ChemblDocumentPipeline,
    stages=("transform",),
)

transform = _STAGES["transform"]
