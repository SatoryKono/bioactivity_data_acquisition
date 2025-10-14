"""Pandera schema for normalised bioactivity data."""
from __future__ import annotations

import pandas as pd
import pandera.pandas as pa

OUTPUT_SCHEMA = pa.DataFrameSchema(
    {
        "assay_id": pa.Column(pa.Int64, checks=pa.Check.ge(0)),
        "molecule_chembl_id": pa.Column(pa.String, checks=pa.Check.str_length(min_value=1)),
        "standard_value_nm": pa.Column(pa.Float64, checks=pa.Check.gt(0)),
        "standard_units": pa.Column(pa.String, checks=pa.Check.isin(["nM"])),
        "activity_comment": pa.Column(pa.String, nullable=True, coerce=True),
        "source": pa.Column(pa.String, checks=pa.Check.str_length(min_value=1)),
    },
    coerce=True,
    strict=True,
)


def validate_output(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate the transformed DataFrame."""
    return OUTPUT_SCHEMA.validate(frame, lazy=True)
