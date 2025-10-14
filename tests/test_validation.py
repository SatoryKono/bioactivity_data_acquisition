from __future__ import annotations

import pandas as pd
import pandera.errors as pa_errors
import pytest

from library.validation.input_schema import INPUT_SCHEMA, validate_input
from library.validation.output_schema import validate_output


def test_validate_input_accepts_valid_frame(sample_frame: pd.DataFrame) -> None:
    validated = validate_input(sample_frame)
    assert isinstance(validated, pd.DataFrame)
    assert list(validated.columns) == list(INPUT_SCHEMA.columns.keys())


def test_validate_input_rejects_invalid_units(sample_frame: pd.DataFrame) -> None:
    sample_frame.loc[0, "standard_units"] = "mg"
    with pytest.raises(pa_errors.SchemaErrors):
        validate_input(sample_frame)


def test_validate_output_requires_nm_units(sample_frame: pd.DataFrame) -> None:
    valid_output = pd.DataFrame(
        {
            "assay_id": [1],
            "molecule_chembl_id": ["CHEMBL1"],
            "standard_value_nm": [100.0],
            "standard_units": ["nM"],
            "activity_comment": [None],
            "source": ["chembl"],
        }
    )
    validated = validate_output(valid_output)
    assert isinstance(validated, pd.DataFrame)

    valid_output.loc[0, "standard_units"] = "uM"
    with pytest.raises(pa_errors.SchemaErrors):
        validate_output(valid_output)
