"""Unit tests for pipeline input Pandera schemas."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.schemas.input_schemas import (
    ActivityInputSchema,
    AssayInputSchema,
    PubChemInputSchema,
)


def test_activity_input_schema_allows_nullable_ids() -> None:
    """Activity schema should accept nullable identifiers and coerce to Int64."""

    df = pd.DataFrame(
        {
            "activity_id": ["1", "2", None],
            "activity_chembl_id": ["CHEMBL1", "CHEMBL2", None],
        }
    )
    validated = ActivityInputSchema.validate(df, lazy=True)
    assert list(validated["activity_id"].astype("Int64")) == [1, 2, pd.NA]


def test_assay_input_schema_coerces_dtype() -> None:
    """Assay schema should coerce identifiers to string dtype."""

    df = pd.DataFrame({"assay_chembl_id": [12345]})
    validated = AssayInputSchema.validate(df, lazy=True)
    assert validated["assay_chembl_id"].astype("string").iloc[0] == "12345"


def test_pubchem_input_schema_requires_chembl_identifier() -> None:
    """PubChem schema enforces presence of molecule_chembl_id column."""

    df = pd.DataFrame(
        {
            "molecule_chembl_id": ["CHEMBL25"],
            "standard_inchi_key": [None],
        }
    )
    validated = PubChemInputSchema.validate(df, lazy=True)
    assert list(validated["molecule_chembl_id"]) == ["CHEMBL25"]

    df_invalid = pd.DataFrame(
        {
            "molecule_chembl_id": ["CHEMBL25", "CHEMBL26"],
            "standard_inchi_key": ["AAABBB", None],
        }
    )
    validated_invalid = PubChemInputSchema.validate(df_invalid, lazy=True)
    assert list(validated_invalid.columns) == ["molecule_chembl_id", "standard_inchi_key"]

