"""Pandera schema describing the normalized ChEMBL testitem (molecule) dataset."""

from __future__ import annotations

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
        "molecule_chembl_id": Column(
            pa.String,
            Check.str_matches(r"^CHEMBL\d+$"),
            nullable=False,
            unique=True,
        ),
        "pref_name": Column(pa.String, nullable=True),
        "molecule_type": Column(pa.String, nullable=True),
        "max_phase": Column(pa.Int64, Check.isin([0, 1, 2, 3, 4]), nullable=True),
        "first_approval": Column(pa.Int64, Check.ge(1900), nullable=True),
        "chirality": Column(pa.Int64, Check.ge(0), nullable=True),
        "black_box_warning": Column(pa.Int64, Check.isin([0, 1]), nullable=True),
        "availability_type": Column(pa.Int64, Check.ge(0), nullable=True),
        "canonical_smiles": Column(pa.String, nullable=True),
        "standard_inchi_key": Column(
            pa.String,
            Check.str_length(27, 27),
            nullable=True,
            unique=True,
        ),
        "full_mwt": Column(pa.Float64, Check.ge(0), nullable=True),
        "mw_freebase": Column(pa.Float64, Check.ge(0), nullable=True),
        "alogp": Column(pa.Float64, nullable=True),
        "hbd": Column(pa.Int64, Check.ge(0), nullable=True),
        "hba": Column(pa.Int64, Check.ge(0), nullable=True),
        "psa": Column(pa.Float64, Check.ge(0), nullable=True),
        "aromatic_rings": Column(pa.Int64, Check.ge(0), nullable=True),
        "rtb": Column(pa.Int64, Check.ge(0), nullable=True),
        "num_ro5_violations": Column(pa.Int64, Check.ge(0), nullable=True),
        "_chembl_db_version": Column(pa.String, nullable=False),
        "_api_version": Column(pa.String, nullable=False),
    },
    strict=True,
    ordered=True,
    coerce=True,
    name=f"TestItemSchema_v{SCHEMA_VERSION}",
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TestItemSchema"]

