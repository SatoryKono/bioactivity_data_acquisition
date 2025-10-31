"""Schema registry coverage for ChEMBL pipelines."""

from __future__ import annotations

import pytest

from bioetl.schemas import (
    ActivitySchema,
    AssaySchema,
    DocumentNormalizedSchema,
    TargetSchema,
    TestItemSchema,
)
from bioetl.schemas.registry import schema_registry
from bioetl.sources.chembl.activity import pipeline as activity_pipeline
from bioetl.sources.chembl.assay import pipeline as assay_pipeline
from bioetl.sources.chembl.document import pipeline as document_pipeline
from bioetl.sources.chembl.target import pipeline as target_pipeline
from bioetl.sources.chembl.testitem import pipeline as testitem_pipeline

# Trigger schema registration side effects for each pipeline module.
_ = (
    activity_pipeline,
    assay_pipeline,
    document_pipeline,
    target_pipeline,
    testitem_pipeline,
)


@pytest.mark.parametrize(
    ("entity", "schema"),
    [
        ("activity", ActivitySchema),
        ("assay", AssaySchema),
        ("document", DocumentNormalizedSchema),
        ("target", TargetSchema),
        ("testitem", TestItemSchema),
    ],
)
def test_schema_registry_contains_expected_entities(entity: str, schema) -> None:
    """Each ChEMBL pipeline should register its canonical Pandera schema."""

    registered = schema_registry.get(entity, "1.0.0")
    assert registered is schema
