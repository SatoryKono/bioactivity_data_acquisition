"""Extract stage wrappers for the ChEMBL activity pipeline."""

from __future__ import annotations

from application.pipelines.specs.chembl.stage_runner import build_stage_functions

from .run import ChemblActivityPipeline

__all__ = ["extract", "extract_all", "extract_by_ids"]

PIPELINE, _STAGES = build_stage_functions(
    ChemblActivityPipeline,
    stages=("extract", "extract_all", "extract_by_ids"),
)

extract = _STAGES["extract"]
extract_all = _STAGES["extract_all"]
extract_by_ids = _STAGES["extract_by_ids"]
