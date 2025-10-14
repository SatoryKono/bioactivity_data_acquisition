"""Tests for Pandera validation schemas."""

from __future__ import annotations

import pandas as pd
import pandas.testing as pdt
import pandera.errors as pa_errors
import pytest

from library.validation import NormalizedBioactivitySchema, RawBioactivitySchema


@pytest.fixture()
def valid_raw_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "compound_id": ["CHEMBL1"],
            "target_pref_name": ["BRAF"],
            "activity_value": [12.3],
            "activity_units": ["nM"],
            "source": ["chembl"],
            "retrieved_at": [pd.Timestamp("2024-01-01T00:00:00Z")],
            "smiles": ["CCO"],
        }
    )


def test_raw_schema_accepts_valid_frame(valid_raw_frame: pd.DataFrame) -> None:
    validated = RawBioactivitySchema.validate(valid_raw_frame, lazy=True)
    pdt.assert_frame_equal(
        validated.reset_index(drop=True),
        valid_raw_frame.reset_index(drop=True),
        check_dtype=False,
    )


def test_raw_schema_rejects_invalid_units(valid_raw_frame: pd.DataFrame) -> None:
    invalid = valid_raw_frame.copy()
    invalid.loc[0, "activity_units"] = "mM"
    with pytest.raises(pa_errors.SchemaErrors):
        RawBioactivitySchema.validate(invalid, lazy=True)


def test_normalized_schema_requires_nanometer_units(valid_raw_frame: pd.DataFrame) -> None:
    normalized = pd.DataFrame(
        {
            "compound_id": ["CHEMBL1"],
            "target": ["BRAF"],
            "activity_value": [12.3],
            "activity_unit": ["nM"],
            "source": ["chembl"],
            "retrieved_at": [pd.Timestamp("2024-01-01T00:00:00Z")],
            "smiles": ["CCO"],
        }
    )
    result = NormalizedBioactivitySchema.validate(normalized, lazy=True)
    pdt.assert_frame_equal(
        result.reset_index(drop=True),
        normalized.reset_index(drop=True),
        check_dtype=False,
    )
