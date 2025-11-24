"""Write stage wrapper for the ChEMBL test item pipeline."""

from __future__ import annotations

from bioetl.pipelines.chembl.stage_runner import build_stage_functions

from .run import TestItemChemblPipeline

__all__ = ["write"]

PIPELINE, _STAGES = build_stage_functions(
    TestItemChemblPipeline,
    stages=("write",),
)

write = _STAGES["write"]
