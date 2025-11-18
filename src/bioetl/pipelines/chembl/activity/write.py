"""Write stage wrapper for the ChEMBL activity pipeline."""

from __future__ import annotations

from functools import partial

from bioetl.pipelines.chembl.stage_runner import register_pipeline, run_stage

from .run import ChemblActivityPipeline

__all__ = ["write"]

PIPELINE = register_pipeline(ChemblActivityPipeline)

write = partial(run_stage, "write", PIPELINE)
