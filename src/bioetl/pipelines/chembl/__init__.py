"""ChEMBL-only pipelines without external sources."""

from bioetl.pipelines.chembl.chembl_activity import ActivityPipeline
from bioetl.pipelines.chembl.chembl_assay import AssayPipeline
from bioetl.pipelines.chembl.chembl_document import DocumentPipeline
from bioetl.pipelines.chembl.chembl_testitem import TestItemPipeline
from bioetl.pipelines.chembl.chembl_target import TargetPipeline

__all__ = [
    "ActivityPipeline",
    "AssayPipeline",
    "DocumentPipeline",
    "TestItemPipeline",
    "TargetPipeline",
]
