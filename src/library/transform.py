"""Transformation utilities for normalising bioactivity data."""
from __future__ import annotations

from typing import Final

import pandas as pd

from .validation.input_schema import validate_input
from .validation.output_schema import validate_output

_UNIT_FACTORS: Final[dict[str, float]] = {"nm": 1.0, "um": 1_000.0, "mm": 1_000_000.0}


def normalise_units(frame: pd.DataFrame, *, strict: bool = True) -> pd.DataFrame:
    """Normalise ``standard_value`` to nanomolar units."""
    source = frame.get("source")
    working = frame.drop(columns=["source"], errors="ignore")
    validated = validate_input(working) if strict else working
    normalised = validated.copy()
    if source is not None:
        normalised["source"] = source
    else:
        normalised["source"] = "unknown"
    normalised["standard_units"] = normalised["standard_units"].str.lower()
    normalised["standard_value_nm"] = normalised.apply(_convert_to_nm, axis=1)
    normalised["standard_units"] = "nM"
    columns = [
        "assay_id",
        "molecule_chembl_id",
        "standard_value_nm",
        "standard_units",
        "activity_comment",
        "source",
    ]
    normalised = normalised.reindex(columns=columns)
    return validate_output(normalised)


def _convert_to_nm(row: pd.Series) -> float:
    units = str(row["standard_units"]).lower()
    factor = _UNIT_FACTORS.get(units)
    if factor is None:
        raise ValueError(f"Unsupported unit: {units}")
    return float(row["standard_value"]) * factor
