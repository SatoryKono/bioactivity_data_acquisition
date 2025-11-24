"""Extract stage wrappers for the ChEMBL target pipeline."""

from __future__ import annotations

from application.pipelines.specs.chembl.stage_runner import build_stage_function

from .run import ChemblTargetPipeline

__all__ = ["extract", "extract_all", "extract_by_ids"]

extract = build_stage_function(ChemblTargetPipeline, "extract")
extract_all = build_stage_function(ChemblTargetPipeline, "extract_all")
extract_by_ids = build_stage_function(ChemblTargetPipeline, "extract_by_ids")
