from __future__ import annotations

import pandas as pd

from bioetl.core.normalizers import (
    IdentifierRule,
    IdentifierStats,
    StringRule,
    StringStats,
    normalize_identifier_columns,
    normalize_string_columns,
)


def test_normalize_identifier_columns_applies_pattern_and_uppercase() -> None:
    df = pd.DataFrame(
        {
            "chembl_id": ["chembl1", "CHEMBL2", "invalid", None],
        },
    )

    normalized_df, stats = normalize_identifier_columns(
        df,
        [IdentifierRule(columns=["chembl_id"], pattern=r"^CHEMBL\d+$")],
    )

    assert list(normalized_df["chembl_id"]) == ["CHEMBL1", "CHEMBL2", pd.NA, None]
    assert stats.per_column["chembl_id"]["normalized"] == 2
    assert stats.per_column["chembl_id"]["invalid"] == 1


def test_normalize_string_columns_supports_title_case_and_max_length() -> None:
    df = pd.DataFrame(
        {
            "name": [" leucine kinase  ", None],
            "label": ["  MIXED  whitespace\n", ""],
        },
    )

    normalized_df, stats = normalize_string_columns(
        df,
        {
            "name": StringRule(title_case=True),
            "label": StringRule(collapse_whitespace=True, max_length=5),
        },
    )

    assert normalized_df.loc[0, "name"] == "Leucine Kinase"
    assert normalized_df.loc[0, "label"] == "MIXED"
    assert pd.isna(normalized_df.loc[1, "label"])
    assert stats.processed == 2


def test_identifier_stats_has_changes_flag() -> None:
    stats = IdentifierStats()

    assert not stats.has_changes

    stats.add("column", normalized_count=3, invalid_count=1)

    assert stats.has_changes
    assert stats.per_column == {"column": {"normalized": 3, "invalid": 1}}


def test_string_stats_has_changes_flag() -> None:
    stats = StringStats()

    assert not stats.has_changes

    stats.add("column", processed_count=2)

    assert stats.has_changes
    assert stats.per_column == {"column": 2}
