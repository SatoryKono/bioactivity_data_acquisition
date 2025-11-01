from __future__ import annotations

from bioetl.schemas import TestItemSchema


def test_column_order_matches_schema_columns():
    """Ensure the declared column order matches the Pandera schema order."""

    expected = TestItemSchema.get_column_order()
    schema_order = list(TestItemSchema.to_schema().columns.keys())
    assert expected == schema_order
