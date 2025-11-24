"""Validate stage wrapper for the ChEMBL assay pipeline."""

from __future__ import annotations

from application.pipelines.specs.chembl.stage_runner import build_stage_functions

from .run import ChemblAssayPipeline

__all__ = ["validate"]

PIPELINE, _STAGES = build_stage_functions(
    ChemblAssayPipeline,
    stages=("validate",),
)

validate = _STAGES["validate"]
