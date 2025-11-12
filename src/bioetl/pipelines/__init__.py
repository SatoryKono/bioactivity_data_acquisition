"""Pipeline orchestration primitives."""

from __future__ import annotations

from bioetl.config.models import CLIConfig

from .base import PipelineBase, RunArtifacts, RunResult, WriteArtifacts, WriteResult
from .chembl.activity.run import ChemblActivityPipeline
from .chembl.assay.run import ChemblAssayPipeline
from .chembl.document.run import ChemblDocumentPipeline
from .chembl.target.run import ChemblTargetPipeline
from .chembl.testitem.run import TestItemChemblPipeline

# ---------------------------------------------------------------------------
# Legacy compatibility aliases
# ---------------------------------------------------------------------------

ActivityPipeline = ChemblActivityPipeline
AssayPipeline = ChemblAssayPipeline
DocumentPipeline = ChemblDocumentPipeline
TargetPipeline = ChemblTargetPipeline
TestItemPipeline = TestItemChemblPipeline
PipelineRunOptions = CLIConfig

__all__ = [
    # Primary orchestrators
    "PipelineBase",
    "RunArtifacts",
    "RunResult",
    "WriteArtifacts",
    "WriteResult",
    # ChEMBL pipeline implementations
    "ChemblActivityPipeline",
    "ChemblAssayPipeline",
    "ChemblDocumentPipeline",
    "ChemblTargetPipeline",
    "TestItemChemblPipeline",
    # Legacy aliases (deprecated; kept for backwards compatibility)
    "ActivityPipeline",
    "AssayPipeline",
    "DocumentPipeline",
    "TargetPipeline",
    "TestItemPipeline",
    "PipelineRunOptions",
]
