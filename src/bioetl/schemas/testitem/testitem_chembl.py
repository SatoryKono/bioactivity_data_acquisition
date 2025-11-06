"""Pandera schema describing the normalized ChEMBL testitem (molecule) dataset."""

from __future__ import annotations

from bioetl.schemas.base import create_schema
from bioetl.schemas.common import (
    chembl_id_column,
    non_nullable_string_column,
    nullable_float64_column,
    nullable_pd_int64_column,
    nullable_string_column,
    string_column_with_check,
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

TestItemSchema = create_schema(
    columns={
        # Scalars
        "molecule_chembl_id": chembl_id_column(nullable=False, unique=True),
        "pref_name": nullable_string_column(),
        "molecule_type": nullable_string_column(),
        "max_phase": nullable_pd_int64_column(isin={0, 1, 2, 3, 4}),
        "first_approval": nullable_pd_int64_column(ge=1900),
        "first_in_class": nullable_pd_int64_column(isin={0, 1}),
        "availability_type": nullable_pd_int64_column(ge=0),
        "black_box_warning": nullable_pd_int64_column(isin={0, 1}),
        "chirality": nullable_pd_int64_column(ge=0),
        "dosed_ingredient": nullable_pd_int64_column(isin={0, 1}),
        "helm_notation": nullable_string_column(),
        "indication_class": nullable_string_column(),
        "inorganic_flag": nullable_pd_int64_column(isin={0, 1}),
        "natural_product": nullable_pd_int64_column(isin={0, 1}),
        "prodrug": nullable_pd_int64_column(isin={0, 1}),
        "structure_type": nullable_string_column(),
        "therapeutic_flag": nullable_pd_int64_column(isin={0, 1}),
        # Flattened from molecule_hierarchy
        "molecule_hierarchy__molecule_chembl_id": nullable_string_column(),
        "molecule_hierarchy__parent_chembl_id": nullable_string_column(),
        # Flattened from molecule_structures
        "molecule_structures__canonical_smiles": nullable_string_column(),
        "molecule_structures__molfile": nullable_string_column(),
        "molecule_structures__standard_inchi": nullable_string_column(),
        "molecule_structures__standard_inchi_key": string_column_with_check(
            str_length=(27, 27), nullable=True, unique=True
        ),
        # Flattened from molecule_properties
        "molecule_properties__alogp": nullable_float64_column(),
        "molecule_properties__aromatic_rings": nullable_pd_int64_column(ge=0),
        "molecule_properties__cx_logd": nullable_float64_column(),
        "molecule_properties__cx_logp": nullable_float64_column(),
        "molecule_properties__cx_most_apka": nullable_float64_column(),
        "molecule_properties__cx_most_bpka": nullable_float64_column(),
        "molecule_properties__full_molformula": nullable_string_column(),
        "molecule_properties__full_mwt": nullable_float64_column(ge=0),
        "molecule_properties__hba": nullable_pd_int64_column(ge=0),
        "molecule_properties__hba_lipinski": nullable_pd_int64_column(ge=0),
        "molecule_properties__hbd": nullable_pd_int64_column(ge=0),
        "molecule_properties__hbd_lipinski": nullable_pd_int64_column(ge=0),
        "molecule_properties__heavy_atoms": nullable_pd_int64_column(ge=0),
        "molecule_properties__molecular_species": nullable_string_column(),
        "molecule_properties__mw_freebase": nullable_float64_column(ge=0),
        "molecule_properties__mw_monoisotopic": nullable_float64_column(ge=0),
        "molecule_properties__num_lipinski_ro5_violations": nullable_pd_int64_column(ge=0),
        "molecule_properties__num_ro5_violations": nullable_pd_int64_column(ge=0),
        "molecule_properties__psa": nullable_float64_column(ge=0),
        "molecule_properties__qed_weighted": nullable_float64_column(ge=0),
        "molecule_properties__ro3_pass": nullable_pd_int64_column(isin={0, 1}),
        "molecule_properties__rtb": nullable_pd_int64_column(ge=0),
        # Serialized arrays
        "atc_classifications": nullable_string_column(),
        "cross_references__flat": nullable_string_column(),
        "molecule_synonyms__flat": nullable_string_column(),
        # Version fields
        "_chembl_db_version": non_nullable_string_column(),
        "_api_version": non_nullable_string_column(),
    },
    version=SCHEMA_VERSION,
    name="TestItemSchema",
    strict=True,
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TestItemSchema"]

