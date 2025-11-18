"""Validate stage wrapper for the ChEMBL activity pipeline."""

from __future__ import annotations

from functools import partial

from bioetl.pipelines.chembl.stage_runner import register_pipeline, run_stage

from .run import ChemblActivityPipeline

__all__ = ["validate"]

PIPELINE = register_pipeline(ChemblActivityPipeline)

validate = partial(run_stage, "validate", PIPELINE)
