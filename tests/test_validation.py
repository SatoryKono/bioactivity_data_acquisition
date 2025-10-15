"""Tests for Pandera validation schemas."""

from __future__ import annotations

import pandas as pd
import pandas.testing as pdt
import pandera.errors as pa_errors
import pytest

from bioactivity.schemas import NormalizedBioactivitySchema, RawBioactivitySchema


@pytest.fixture()
def valid_input_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "assay_id": [12345],
            "molecule_chembl_id": ["CHEMBL1"],
            "standard_value": [12.3],
            "standard_units": ["nM"],
            "activity_comment": ["active"],
        }
    )


@pytest.fixture()
def valid_output_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "assay_id": [12345],
            "molecule_chembl_id": ["CHEMBL1"],
            "standard_value_nm": [12.3],
            "standard_units": ["nM"],
            "activity_comment": ["active"],
            "source": ["chembl"],
        }
    )


def test_input_schema_accepts_valid_frame(valid_input_frame: pd.DataFrame) -> None:
    schema = RawBioactivitySchema.to_schema()
    validated = schema.validate(valid_input_frame, lazy=True)
    pdt.assert_frame_equal(
        validated.reset_index(drop=True),
        valid_input_frame.reset_index(drop=True),
        check_dtype=False,
    )


def test_input_schema_rejects_invalid_units(valid_input_frame: pd.DataFrame) -> None:
    invalid = valid_input_frame.copy()
    invalid.loc[0, "standard_units"] = "mg/mL"
    with pytest.raises(pa_errors.SchemaErrors):
        schema.validate(invalid, lazy=True)


def test_output_schema_requires_nanometer_units(valid_output_frame: pd.DataFrame) -> None:
    schema = NormalizedBioactivitySchema.to_schema()
    result = schema.validate(valid_output_frame, lazy=True)
    pdt.assert_frame_equal(
        result.reset_index(drop=True),
        valid_output_frame.reset_index(drop=True),
        check_dtype=False,
    )
