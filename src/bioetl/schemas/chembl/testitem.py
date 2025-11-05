"""Pandera schema describing the normalized ChEMBL testitem (molecule) dataset."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera import Check, Column, DataFrameSchema

SCHEMA_VERSION = "1.0.0"

COLUMN_ORDER = (
    "molecule_chembl_id",
    "pref_name",
    "molecule_type",
    "max_phase",
    "first_approval",
    "chirality",
    "black_box_warning",
    "availability_type",
    "canonical_smiles",
    "standard_inchi_key",
    "full_mwt",
    "mw_freebase",
    "alogp",
    "hbd",
    "hba",
    "psa",
    "aromatic_rings",
    "rtb",
    "num_ro5_violations",
    "_chembl_db_version",
    "_api_version",
)

TestItemSchema = DataFrameSchema(
    {
        "molecule_chembl_id": Column(  # type: ignore[assignment]
            pa.String,  # type: ignore[arg-type]
            Check.str_matches(r"^CHEMBL\d+$"),  # type: ignore[arg-type]
            nullable=False,
            unique=True,
        ),
        "pref_name": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "molecule_type": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "max_phase": Column(pd.Int64Dtype(), Check.isin([0, 1, 2, 3, 4]), nullable=True),  # type: ignore[arg-type]
        "first_approval": Column(pd.Int64Dtype(), Check.ge(1900), nullable=True),  # type: ignore[arg-type]
        "chirality": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "black_box_warning": Column(pd.Int64Dtype(), Check.isin([0, 1]), nullable=True),  # type: ignore[arg-type]
        "availability_type": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "canonical_smiles": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "standard_inchi_key": Column(  # type: ignore[assignment]
            pa.String,  # type: ignore[arg-type]
            Check.str_length(27, 27),  # type: ignore[arg-type]
            nullable=True,
            unique=True,
        ),
        "full_mwt": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "mw_freebase": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "alogp": Column(pa.Float64, nullable=True),  # type: ignore[assignment]
        "hbd": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "hba": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "psa": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "aromatic_rings": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "rtb": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "num_ro5_violations": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "_chembl_db_version": Column(pa.String, nullable=False),  # type: ignore[assignment]
        "_api_version": Column(pa.String, nullable=False),  # type: ignore[assignment]
    },
    strict=True,
    ordered=True,
    coerce=False,  # Disable coercion at schema level - types are normalized in transform
    name=f"TestItemSchema_v{SCHEMA_VERSION}",
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TestItemSchema"]

