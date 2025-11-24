"""Write stage wrapper for the ChEMBL document pipeline."""

from __future__ import annotations

from application.pipelines.specs.chembl.stage_runner import build_stage_function

from .run import ChemblDocumentPipeline

__all__ = ["write"]

write = build_stage_function(ChemblDocumentPipeline, "write")
