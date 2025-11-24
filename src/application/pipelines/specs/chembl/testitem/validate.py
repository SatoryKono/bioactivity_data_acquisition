"""Validate stage wrapper for the ChEMBL test item pipeline."""

from __future__ import annotations

from application.pipelines.specs.chembl.stage_runner import build_stage_function

from .run import TestItemChemblPipeline

__all__ = ["validate"]

validate = build_stage_function(TestItemChemblPipeline, "validate")
