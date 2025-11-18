"""Extract stage wrappers for the ChEMBL activity pipeline."""

from __future__ import annotations

from functools import partial

from bioetl.pipelines.chembl.stage_runner import register_pipeline, run_stage

from .run import ChemblActivityPipeline

__all__ = ["extract", "extract_all", "extract_by_ids"]

PIPELINE = register_pipeline(ChemblActivityPipeline)

extract = partial(run_stage, "extract", PIPELINE)
extract_all = partial(run_stage, "extract_all", PIPELINE)
extract_by_ids = partial(run_stage, "extract_by_ids", PIPELINE)
