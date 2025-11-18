"""Validate stage wrapper for the ChEMBL target pipeline."""

from __future__ import annotations

from functools import partial

from bioetl.pipelines.chembl.stage_runner import register_pipeline, run_stage

from .run import ChemblTargetPipeline

__all__ = ["validate"]

PIPELINE = register_pipeline(ChemblTargetPipeline)

validate = partial(run_stage, "validate", PIPELINE)
