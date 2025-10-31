"""Sanity checks for ChEMBL pipeline helpers."""

from __future__ import annotations

from bioetl.sources.chembl.activity.pipeline import _get_activity_column_order


def test_activity_column_order_contains_business_key() -> None:
    """The canonical column order should include the business key fields."""

    columns = _get_activity_column_order()
    assert "activity_id" in columns
    assert "molecule_chembl_id" in columns
