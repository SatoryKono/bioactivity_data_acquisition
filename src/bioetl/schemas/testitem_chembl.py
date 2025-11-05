"""Pandera schema describing the normalized ChEMBL testitem (molecule) dataset."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera import Check, Column

from ._common import (
    boolean_flag_column,
    chembl_id_column,
    create_chembl_schema,
    inchi_key_column,
    non_negative_float_column,
    non_negative_int_column,
    standard_string_column,
)

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
    "molecule_hierarchy__parent_chembl_id",
    # Flattened from molecule_structures
    "molecule_structures__canonical_smiles",
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

TestItemSchema = create_chembl_schema(
    {
        # Scalars
        "molecule_chembl_id": chembl_id_column(nullable=False, unique=True),
        "pref_name": standard_string_column(),
        "molecule_type": standard_string_column(),
        "max_phase": Column(pd.Int64Dtype(), Check.isin([0, 1, 2, 3, 4]), nullable=True),  # type: ignore[arg-type]
        "first_approval": Column(pd.Int64Dtype(), Check.ge(1900), nullable=True),  # type: ignore[arg-type]
        "first_in_class": boolean_flag_column(),
        "availability_type": non_negative_int_column(dtype=pd.Int64Dtype()),
        "black_box_warning": boolean_flag_column(),
        "chirality": non_negative_int_column(dtype=pd.Int64Dtype()),
        "dosed_ingredient": boolean_flag_column(),
        "helm_notation": standard_string_column(),
        "indication_class": standard_string_column(),
        "inorganic_flag": boolean_flag_column(),
        "natural_product": boolean_flag_column(),
        "prodrug": boolean_flag_column(),
        "structure_type": standard_string_column(),
        "therapeutic_flag": boolean_flag_column(),
        # Flattened from molecule_hierarchy
        "molecule_hierarchy__parent_chembl_id": chembl_id_column(),
        # Flattened from molecule_structures
        "molecule_structures__canonical_smiles": standard_string_column(),
        "molecule_structures__molfile": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "molecule_structures__standard_inchi": standard_string_column(),
        "molecule_structures__standard_inchi_key": inchi_key_column(unique=True),
        # Flattened from molecule_properties
        "molecule_properties__alogp": Column(pa.Float64, nullable=True),  # type: ignore[assignment]
        "molecule_properties__aromatic_rings": non_negative_int_column(dtype=pd.Int64Dtype()),
        "molecule_properties__cx_logd": Column(pa.Float64, nullable=True),  # type: ignore[assignment]
        "molecule_properties__cx_logp": Column(pa.Float64, nullable=True),  # type: ignore[assignment]
        "molecule_properties__cx_most_apka": Column(pa.Float64, nullable=True),  # type: ignore[assignment]
        "molecule_properties__cx_most_bpka": Column(pa.Float64, nullable=True),  # type: ignore[assignment]
        "molecule_properties__full_molformula": standard_string_column(),
        "molecule_properties__full_mwt": non_negative_float_column(),
        "molecule_properties__hba": non_negative_int_column(dtype=pd.Int64Dtype()),
        "molecule_properties__hba_lipinski": non_negative_int_column(dtype=pd.Int64Dtype()),
        "molecule_properties__hbd": non_negative_int_column(dtype=pd.Int64Dtype()),
        "molecule_properties__hbd_lipinski": non_negative_int_column(dtype=pd.Int64Dtype()),
        "molecule_properties__heavy_atoms": non_negative_int_column(dtype=pd.Int64Dtype()),
        "molecule_properties__molecular_species": standard_string_column(),
        "molecule_properties__mw_freebase": non_negative_float_column(),
        "molecule_properties__mw_monoisotopic": non_negative_float_column(),
        "molecule_properties__num_lipinski_ro5_violations": non_negative_int_column(dtype=pd.Int64Dtype()),
        "molecule_properties__num_ro5_violations": non_negative_int_column(dtype=pd.Int64Dtype()),
        "molecule_properties__psa": non_negative_float_column(),
        "molecule_properties__qed_weighted": non_negative_float_column(),
        "molecule_properties__ro3_pass": boolean_flag_column(),
        "molecule_properties__rtb": non_negative_int_column(dtype=pd.Int64Dtype()),
        # Serialized arrays
        "atc_classifications": standard_string_column(),
        "cross_references__flat": standard_string_column(),
        "molecule_synonyms__flat": standard_string_column(),
        # Version fields
        "_chembl_db_version": standard_string_column(nullable=False),
        "_api_version": standard_string_column(nullable=False),
    },
    schema_name="TestItemSchema",
    version=SCHEMA_VERSION,
    strict=True,
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TestItemSchema"]

