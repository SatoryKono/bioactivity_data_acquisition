from __future__ import annotations

import pytest

from bioetl.schemas.document import DocumentNormalizedSchema, DocumentSchema


@pytest.mark.parametrize(
    "schema_cls",
    [DocumentSchema, DocumentNormalizedSchema],
)
def test_config_column_order_matches_get_column_order(schema_cls):
    """Config.column_order should expose the canonical schema order lazily."""

    expected = schema_cls.get_column_order()
    assert schema_cls.Config.column_order == expected


@pytest.mark.parametrize(
    "schema_cls",
    [DocumentSchema, DocumentNormalizedSchema],
)
def test_to_schema_handles_column_order_accessor(schema_cls):
    """Calling to_schema should not raise when column_order accessor is present."""

    schema = schema_cls.to_schema()
    assert list(schema.columns.keys())


def test_document_schemas_disable_pandera_column_order_enforcement():
    """Normalized document schemas must not rely on Pandera's order checks."""

    assert DocumentSchema.Config.ordered is False
    assert DocumentNormalizedSchema.Config.ordered is False
