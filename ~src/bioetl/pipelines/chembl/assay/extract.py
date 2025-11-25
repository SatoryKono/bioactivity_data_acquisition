"""Extract stage wrappers for the ChEMBL assay pipeline."""

from __future__ import annotations

from bioetl.pipelines.chembl.stage_runner import build_stage_functions

from .run import ChemblAssayPipeline

__all__ = ["extract", "extract_all", "extract_by_ids"]

PIPELINE, _STAGES = build_stage_functions(
    ChemblAssayPipeline,
    stages=("extract", "extract_all", "extract_by_ids"),
)

extract = _STAGES["extract"]
extract_all = _STAGES["extract_all"]
extract_by_ids = _STAGES["extract_by_ids"]
