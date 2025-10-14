"""Tests for data normalization logic."""

from __future__ import annotations

import pandas as pd
import pandera.errors as pa_errors
import pytest

from library.pipeline.transform import normalize_bioactivity_data


def _raw_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "compound_id": ["CHEMBL1", "CHEMBL2"],
            "target_pref_name": ["BRAF", "EGFR"],
            "activity_value": [1.0, 2.0],
            "activity_units": ["uM", "nM"],
            "source": ["chembl", "chembl"],
            "retrieved_at": [
                pd.Timestamp("2024-01-01T00:00:00Z"),
                pd.Timestamp("2024-01-02T00:00:00Z"),
            ],
            "smiles": ["CCO", None],
        }
    )


def test_normalize_converts_units_and_orders_rows() -> None:
    raw = _raw_frame()
    normalized = normalize_bioactivity_data(raw)
    assert list(normalized.columns) == [
        "compound_id",
        "target",
        "activity_value",
        "activity_unit",
        "source",
        "retrieved_at",
        "smiles",
    ]
    assert normalized.loc[0, "compound_id"] == "CHEMBL1"
    assert normalized.loc[0, "activity_value"] == pytest.approx(1000.0)
    assert (normalized["activity_unit"] == "nM").all()
    assert normalized.loc[1, "activity_value"] == pytest.approx(2.0)
    assert pd.api.types.is_datetime64_ns_dtype(normalized["retrieved_at"])


def test_normalize_raises_on_unknown_units() -> None:
    raw = _raw_frame()
    raw.loc[0, "activity_units"] = "mM"
    with pytest.raises(pa_errors.SchemaErrors):
        normalize_bioactivity_data(raw)


