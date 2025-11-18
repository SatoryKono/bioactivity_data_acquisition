"""Extract stage wrappers for the ChEMBL test item pipeline."""

from __future__ import annotations

from bioetl.pipelines.chembl.stage_runner import build_stage_functions

from .run import TestItemChemblPipeline

__all__ = ["extract", "extract_all", "extract_by_ids"]

PIPELINE, _STAGES = build_stage_functions(
    TestItemChemblPipeline,
    stages=("extract", "extract_all", "extract_by_ids"),
)

extract = _STAGES["extract"]
extract_all = _STAGES["extract_all"]
extract_by_ids = _STAGES["extract_by_ids"]
