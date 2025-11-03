"""Sanity checks for ChEMBL pipeline helpers."""

from __future__ import annotations

from bioetl.schemas.activity import COLUMN_ORDER


def test_activity_column_order_contains_business_key() -> None:
    """The canonical column order should include the business key fields."""

    columns = COLUMN_ORDER
    assert "activity_id" in columns
    assert "molecule_chembl_id" in columns
    assert "assay_id" in columns
