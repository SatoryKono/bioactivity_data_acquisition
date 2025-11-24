"""Write stage wrapper for the ChEMBL assay pipeline."""

from __future__ import annotations

from application.pipelines.specs.chembl.stage_runner import build_stage_function

from .run import ChemblAssayPipeline

__all__ = ["write"]

write = build_stage_function(ChemblAssayPipeline, "write")
