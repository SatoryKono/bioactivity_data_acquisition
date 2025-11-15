"""Unit tests for shared normalization helpers."""

from __future__ import annotations

import pandas as pd

from bioetl.core.schema import IdentifierRule
from bioetl.chembl.common.normalize import (
    RowMetadataChanges,
    add_row_metadata,
    normalize_identifiers,
)


def test_normalize_identifiers_applies_rules() -> None:
    """Identifiers are upper-cased and invalid values are nulled."""

    df = pd.DataFrame(
        {
            "assay_chembl_id": ["chembl1", "CHEMBL2", "invalid"],
            "bao_format": ["BAO_0000001", "bao_0000002", "bad"],
        }
    )

    rules = [
        IdentifierRule(columns=["assay_chembl_id"], pattern=r"^CHEMBL\d+$"),
        IdentifierRule(columns=["bao_format"], pattern=r"^BAO_\d{7}$"),
    ]

    normalized, stats = normalize_identifiers(df, rules)

    assert normalized["assay_chembl_id"].tolist() == ["CHEMBL1", "CHEMBL2", pd.NA]
    assert normalized["bao_format"].tolist() == ["BAO_0000001", "BAO_0000002", pd.NA]
    assert stats.has_changes
    assert stats.per_column["assay_chembl_id"]["invalid"] == 1
    assert stats.per_column["bao_format"]["normalized"] == 2


def test_normalize_identifiers_no_rules_returns_copy() -> None:
    """When no rules are supplied the original frame is preserved."""

    df = pd.DataFrame({"target_chembl_id": ["CHEMBL1"]})

    normalized, stats = normalize_identifiers(df, [])

    assert normalized.equals(df)
    assert not stats.has_changes


def test_add_row_metadata_adds_missing_columns() -> None:
    """Missing metadata columns are created with expected values."""

    df = pd.DataFrame({"activity_id": [1, 2, 3]})

    result, metadata = add_row_metadata(df, subtype="activity")

    assert isinstance(metadata, RowMetadataChanges)
    assert metadata.subtype_added and not metadata.subtype_filled
    assert metadata.index_added and not metadata.index_filled
    assert result["row_subtype"].tolist() == ["activity", "activity", "activity"]
    assert result["row_index"].tolist() == [0, 1, 2]


def test_add_row_metadata_fills_existing_null_columns() -> None:
    """Existing metadata columns comprised of nulls are populated."""

    df = pd.DataFrame(
        {
            "row_subtype": [pd.NA, pd.NA],
            "row_index": [pd.NA, pd.NA],
        }
    )

    result, metadata = add_row_metadata(df, subtype="assay")

    assert not metadata.subtype_added and metadata.subtype_filled
    assert not metadata.index_added and metadata.index_filled
    assert result["row_subtype"].tolist() == ["assay", "assay"]
    assert result["row_index"].tolist() == [0, 1]
