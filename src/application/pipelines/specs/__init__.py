"""Pipeline orchestration primitives."""

from __future__ import annotations

from infrastructure.config.models.models import CLIConfig
from infrastructure.io import RunArtifacts, WriteArtifacts, WriteResult
from application.pipelines import PipelineBase, RunResult
from application.pipelines.specs.chembl.activity.run import ChemblActivityPipeline
from application.pipelines.specs.chembl.assay.run import ChemblAssayPipeline
from application.pipelines.specs.chembl.document.run import ChemblDocumentPipeline
from application.pipelines.specs.chembl.target.run import ChemblTargetPipeline
from application.pipelines.specs.chembl.testitem.run import TestItemChemblPipeline

PipelineRunOptions = CLIConfig

ActivityPipeline = ChemblActivityPipeline
AssayPipeline = ChemblAssayPipeline
DocumentPipeline = ChemblDocumentPipeline
TargetPipeline = ChemblTargetPipeline
TestItemPipeline = TestItemChemblPipeline

__all__ = [
    "PipelineBase",
    "RunArtifacts",
    "RunResult",
    "WriteArtifacts",
    "WriteResult",
    "PipelineRunOptions",
    "ChemblActivityPipeline",
    "ChemblAssayPipeline",
    "ChemblDocumentPipeline",
    "ChemblTargetPipeline",
    "TestItemChemblPipeline",
    "ActivityPipeline",
    "AssayPipeline",
    "DocumentPipeline",
    "TargetPipeline",
    "TestItemPipeline",
]
