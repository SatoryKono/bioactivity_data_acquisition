"""Write stage wrapper for the ChEMBL assay pipeline."""

from __future__ import annotations

from functools import partial

from bioetl.pipelines.chembl.stage_runner import register_pipeline, run_stage

from .run import ChemblAssayPipeline

__all__ = ["write"]

PIPELINE = register_pipeline(ChemblAssayPipeline)

write = partial(run_stage, "write", PIPELINE)
