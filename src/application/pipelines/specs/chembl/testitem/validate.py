"""Validate stage wrapper for the ChEMBL test item pipeline."""

from __future__ import annotations

from application.pipelines.specs.chembl.stage_runner import build_stage_functions

from .run import TestItemChemblPipeline

__all__ = ["validate"]

PIPELINE, _STAGES = build_stage_functions(
    TestItemChemblPipeline,
    stages=("validate",),
)

validate = _STAGES["validate"]
