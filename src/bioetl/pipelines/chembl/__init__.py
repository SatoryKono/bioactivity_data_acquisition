"""ChEMBL stage helpers and thin wrappers."""

from bioetl.application.pipelines.chembl.stage_runner import run_chembl_stage
try:  # pragma: no cover - optional heavy dependencies
    from .activity import ChemblActivityPipeline
    from .assay import ChemblAssayPipeline
    from .document import ChemblDocumentPipeline
    from .target import ChemblTargetPipeline
    from .testitem import TestItemChemblPipeline
    from .thin import (
        ChemblActivityThinPipeline,
        ChemblAssayThinPipeline,
        ChemblDocumentThinPipeline,
        ChemblTargetThinPipeline,
        ChemblTestItemThinPipeline,
    )
except Exception:  # pragma: no cover - allow lightweight imports
    ChemblActivityPipeline = None
    ChemblAssayPipeline = None
    ChemblDocumentPipeline = None
    ChemblTargetPipeline = None
    TestItemChemblPipeline = None
    ChemblActivityThinPipeline = None
    ChemblAssayThinPipeline = None
    ChemblDocumentThinPipeline = None
    ChemblTargetThinPipeline = None
    ChemblTestItemThinPipeline = None

__all__ = [
    "run_chembl_stage",
    "ChemblActivityPipeline",
    "ChemblAssayPipeline",
    "ChemblDocumentPipeline",
    "ChemblTargetPipeline",
    "TestItemChemblPipeline",
    "ChemblActivityThinPipeline",
    "ChemblAssayThinPipeline",
    "ChemblDocumentThinPipeline",
    "ChemblTargetThinPipeline",
    "ChemblTestItemThinPipeline",
]

