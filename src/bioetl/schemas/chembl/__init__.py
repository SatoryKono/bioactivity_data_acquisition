"""ChEMBL-only Pandera schemas."""

from __future__ import annotations

from bioetl.schemas.chembl.activity import ActivitySchema  # type: ignore[assignment]
from bioetl.schemas.chembl.assay import AssaySchema  # type: ignore[assignment]
from bioetl.schemas.chembl.document import DocumentSchema  # type: ignore[assignment]
from bioetl.schemas.chembl.testitem import TestItemSchema  # type: ignore[assignment]
from bioetl.schemas.chembl.target import TargetSchema  # type: ignore[assignment]

__all__ = [
    "ActivitySchema",
    "AssaySchema",
    "DocumentSchema",
    "TestItemSchema",
    "TargetSchema",
]

