"""Validate stage wrapper for the ChEMBL document pipeline."""

from __future__ import annotations

from bioetl.pipelines.chembl.stage_runner import build_stage_functions

from .run import ChemblDocumentPipeline

__all__ = ["validate"]

PIPELINE, _STAGES = build_stage_functions(
    ChemblDocumentPipeline,
    stages=("validate",),
)

validate = _STAGES["validate"]
