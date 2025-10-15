"""Tests for Pandera validation schemas."""

from __future__ import annotations

import pandas as pd
import pandas.testing as pdt
import pandera.errors as pa_errors
import pytest

from library.schemas import NormalizedBioactivitySchema, RawBioactivitySchema


@pytest.fixture()
def valid_input_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "compound_id": ["CHEMBL1"],
            "target_pref_name": ["Protein X"],
            "activity_value": [12.3],
            "activity_units": ["nM"],
            "source": ["chembl"],
            "retrieved_at": pd.to_datetime(["2024-01-01T00:00:00Z"], utc=True),
            "smiles": ["C1=CC=CC=C1"],
        }
    )


@pytest.fixture()
def valid_output_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "compound_id": ["CHEMBL1"],
            "target": ["Protein X"],
            "activity_value": [12.3],
            "activity_unit": ["nM"],
            "source": ["chembl"],
            "retrieved_at": pd.to_datetime(["2024-01-01T00:00:00Z"], utc=True),
            "smiles": ["C1=CC=CC=C1"],
        }
    )


def test_input_schema_accepts_valid_frame(valid_input_frame: pd.DataFrame) -> None:
    schema = RawBioactivitySchema.to_schema()
    validated = schema.validate(valid_input_frame, lazy=True)
    expected = valid_input_frame.copy()
    expected["retrieved_at"] = pd.to_datetime(expected["retrieved_at"], utc=True)
    pdt.assert_frame_equal(
        validated.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_input_schema_rejects_invalid_units(valid_input_frame: pd.DataFrame) -> None:
    schema = RawBioactivitySchema.to_schema()
    invalid = valid_input_frame.copy()
    invalid.loc[0, "activity_units"] = "mg/mL"
    with pytest.raises(pa_errors.SchemaErrors):
        schema.validate(invalid, lazy=True)


def test_output_schema_requires_nanometer_units(valid_output_frame: pd.DataFrame) -> None:
    schema = NormalizedBioactivitySchema.to_schema()
    result = schema.validate(valid_output_frame, lazy=True)
    expected = valid_output_frame.copy()
    expected["retrieved_at"] = pd.to_datetime(expected["retrieved_at"], utc=True)
    pdt.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )
