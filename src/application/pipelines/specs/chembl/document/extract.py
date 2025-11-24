"""Extract stage wrappers for the ChEMBL document pipeline."""

from __future__ import annotations

from application.pipelines.specs.chembl.stage_runner import build_stage_function

from .run import ChemblDocumentPipeline

__all__ = ["extract", "extract_all", "extract_by_ids"]

extract = build_stage_function(ChemblDocumentPipeline, "extract")
extract_all = build_stage_function(ChemblDocumentPipeline, "extract_all")
extract_by_ids = build_stage_function(ChemblDocumentPipeline, "extract_by_ids")
