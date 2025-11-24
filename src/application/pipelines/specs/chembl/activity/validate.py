"""Validate stage wrapper for the ChEMBL activity pipeline."""

from __future__ import annotations

from application.pipelines.specs.chembl.stage_runner import build_stage_function

from .run import ChemblActivityPipeline

__all__ = ["validate"]

validate = build_stage_function(ChemblActivityPipeline, "validate")
