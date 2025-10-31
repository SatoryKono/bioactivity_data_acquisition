"""Schema registry coverage for ChEMBL pipelines."""

from __future__ import annotations

import pytest

from bioetl.schemas import ActivitySchema, AssaySchema, DocumentSchema, TargetSchema, TestItemSchema
from bioetl.schemas.registry import schema_registry


@pytest.mark.parametrize(
    ("entity", "schema"),
    [
        ("activity", ActivitySchema),
        ("assay", AssaySchema),
        ("document", DocumentSchema),
        ("target", TargetSchema),
        ("testitem", TestItemSchema),
    ],
)
def test_schema_registry_contains_expected_entities(entity: str, schema) -> None:
    """Each ChEMBL pipeline should register its canonical Pandera schema."""

    registered = schema_registry.get(entity, "1.0.0")
    assert registered is schema
