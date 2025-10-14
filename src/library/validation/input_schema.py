"""Pandera schema for input bioactivity data."""
from __future__ import annotations

import pandas as pd
import pandera.pandas as pa

INPUT_SCHEMA = pa.DataFrameSchema(
    {
        "assay_id": pa.Column(pa.Int64, checks=pa.Check.ge(0)),
        "molecule_chembl_id": pa.Column(pa.String, checks=pa.Check.str_length(min_value=1)),
        "standard_value": pa.Column(pa.Float64, checks=pa.Check.gt(0)),
        "standard_units": pa.Column(pa.String, checks=pa.Check.isin(["nM", "uM", "mM"])),
        "activity_comment": pa.Column(pa.String, nullable=True, coerce=True),
    },
    coerce=True,
    strict=True,
)


def validate_input(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate an incoming DataFrame against ``INPUT_SCHEMA``."""
    return INPUT_SCHEMA.validate(frame, lazy=True)
