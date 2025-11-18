"""Transform stage wrapper for the ChEMBL activity pipeline."""

from __future__ import annotations

from functools import partial

from bioetl.pipelines.chembl.stage_runner import register_pipeline, run_stage

from .run import ChemblActivityPipeline

__all__ = ["transform"]

PIPELINE = register_pipeline(ChemblActivityPipeline)

transform = partial(run_stage, "transform", PIPELINE)
