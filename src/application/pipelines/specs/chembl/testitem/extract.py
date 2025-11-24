"""Extract stage wrappers for the ChEMBL test item pipeline."""

from __future__ import annotations

from application.pipelines.specs.chembl.stage_runner import build_stage_function

from .run import TestItemChemblPipeline

__all__ = ["extract", "extract_all", "extract_by_ids"]

extract = build_stage_function(TestItemChemblPipeline, "extract")
extract_all = build_stage_function(TestItemChemblPipeline, "extract_all")
extract_by_ids = build_stage_function(TestItemChemblPipeline, "extract_by_ids")
