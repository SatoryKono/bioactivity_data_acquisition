"""Pandera schema describing the normalized ChEMBL testitem (molecule) dataset."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera import Check, Column, DataFrameSchema

SCHEMA_VERSION = "1.0.0"

COLUMN_ORDER = (
    # Scalars
    "molecule_chembl_id",
    "pref_name",
    "molecule_type",
    "max_phase",
    "first_approval",
    "first_in_class",
    "availability_type",
    "black_box_warning",
    "chirality",
    "dosed_ingredient",
    "helm_notation",
    "indication_class",
    "inorganic_flag",
    "natural_product",
    "prodrug",
    "structure_type",
    "therapeutic_flag",
    # Flattened from molecule_hierarchy
    "molecule_hierarchy__molecule_chembl_id",
    "molecule_hierarchy__parent_chembl_id",
    # Flattened from molecule_structures
    "molecule_structures__canonical_smiles",
    "molecule_structures__molfile",
    "molecule_structures__standard_inchi",
    "molecule_structures__standard_inchi_key",
    # Flattened from molecule_properties
    "molecule_properties__alogp",
    "molecule_properties__aromatic_rings",
    "molecule_properties__cx_logd",
    "molecule_properties__cx_logp",
    "molecule_properties__cx_most_apka",
    "molecule_properties__cx_most_bpka",
    "molecule_properties__full_molformula",
    "molecule_properties__full_mwt",
    "molecule_properties__hba",
    "molecule_properties__hba_lipinski",
    "molecule_properties__hbd",
    "molecule_properties__hbd_lipinski",
    "molecule_properties__heavy_atoms",
    "molecule_properties__molecular_species",
    "molecule_properties__mw_freebase",
    "molecule_properties__mw_monoisotopic",
    "molecule_properties__num_lipinski_ro5_violations",
    "molecule_properties__num_ro5_violations",
    "molecule_properties__psa",
    "molecule_properties__qed_weighted",
    "molecule_properties__ro3_pass",
    "molecule_properties__rtb",
    # Serialized arrays
    "atc_classifications",
    "cross_references__flat",
    "molecule_synonyms__flat",
    # Version fields
    "_chembl_db_version",
    "_api_version",
)

TestItemSchema = DataFrameSchema(
    {
        # Scalars
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
        "first_in_class": Column(pd.Int64Dtype(), Check.isin([0, 1]), nullable=True),  # type: ignore[arg-type]
        "availability_type": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "black_box_warning": Column(pd.Int64Dtype(), Check.isin([0, 1]), nullable=True),  # type: ignore[arg-type]
        "chirality": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "dosed_ingredient": Column(pd.Int64Dtype(), Check.isin([0, 1]), nullable=True),  # type: ignore[arg-type]
        "helm_notation": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "indication_class": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "inorganic_flag": Column(pd.Int64Dtype(), Check.isin([0, 1]), nullable=True),  # type: ignore[arg-type]
        "natural_product": Column(pd.Int64Dtype(), Check.isin([0, 1]), nullable=True),  # type: ignore[arg-type]
        "prodrug": Column(pd.Int64Dtype(), Check.isin([0, 1]), nullable=True),  # type: ignore[arg-type]
        "structure_type": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "therapeutic_flag": Column(pd.Int64Dtype(), Check.isin([0, 1]), nullable=True),  # type: ignore[arg-type]
        # Flattened from molecule_hierarchy
        "molecule_hierarchy__molecule_chembl_id": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "molecule_hierarchy__parent_chembl_id": Column(pa.String, nullable=True),  # type: ignore[assignment]
        # Flattened from molecule_structures
        "molecule_structures__canonical_smiles": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "molecule_structures__molfile": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "molecule_structures__standard_inchi": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "molecule_structures__standard_inchi_key": Column(  # type: ignore[assignment]
            pa.String,  # type: ignore[arg-type]
            Check.str_length(27, 27),  # type: ignore[arg-type]
            nullable=True,
            unique=True,
        ),
        # Flattened from molecule_properties
        "molecule_properties__alogp": Column(pa.Float64, nullable=True),  # type: ignore[assignment]
        "molecule_properties__aromatic_rings": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "molecule_properties__cx_logd": Column(pa.Float64, nullable=True),  # type: ignore[assignment]
        "molecule_properties__cx_logp": Column(pa.Float64, nullable=True),  # type: ignore[assignment]
        "molecule_properties__cx_most_apka": Column(pa.Float64, nullable=True),  # type: ignore[assignment]
        "molecule_properties__cx_most_bpka": Column(pa.Float64, nullable=True),  # type: ignore[assignment]
        "molecule_properties__full_molformula": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "molecule_properties__full_mwt": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "molecule_properties__hba": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "molecule_properties__hba_lipinski": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "molecule_properties__hbd": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "molecule_properties__hbd_lipinski": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "molecule_properties__heavy_atoms": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "molecule_properties__molecular_species": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "molecule_properties__mw_freebase": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "molecule_properties__mw_monoisotopic": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "molecule_properties__num_lipinski_ro5_violations": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "molecule_properties__num_ro5_violations": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        "molecule_properties__psa": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "molecule_properties__qed_weighted": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "molecule_properties__ro3_pass": Column(pd.Int64Dtype(), Check.isin([0, 1]), nullable=True),  # type: ignore[arg-type]
        "molecule_properties__rtb": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
        # Serialized arrays
        "atc_classifications": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "cross_references__flat": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "molecule_synonyms__flat": Column(pa.String, nullable=True),  # type: ignore[assignment]
        # Version fields
        "_chembl_db_version": Column(pa.String, nullable=False),  # type: ignore[assignment]
        "_api_version": Column(pa.String, nullable=False),  # type: ignore[assignment]
    },
    strict=True,
    ordered=True,
    coerce=False,  # Disable coercion at schema level - types are normalized in transform
    name=f"TestItemSchema_v{SCHEMA_VERSION}",
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TestItemSchema"]

