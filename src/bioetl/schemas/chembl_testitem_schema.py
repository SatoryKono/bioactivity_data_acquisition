"""Pandera schema describing the normalized ChEMBL testitem (molecule) dataset."""

from __future__ import annotations

from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import SchemaColumnFactory

SCHEMA_VERSION = "1.2.0"

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
    "hash_row",
    "hash_business_key",
    "load_meta_id",
)

CF = SchemaColumnFactory

TestItemSchema = create_schema(
    columns={
        # Scalars
        "molecule_chembl_id": CF.chembl_id(nullable=False, unique=True),
        "pref_name": CF.string(),
        "molecule_type": CF.string(),
        "max_phase": CF.int64(pandas_nullable=True, isin={0, 1, 2, 3, 4}),
        "first_approval": CF.int64(pandas_nullable=True, ge=1900),
        "first_in_class": CF.int64(pandas_nullable=True, isin={0, 1}),
        "availability_type": CF.int64(pandas_nullable=True, ge=0),
        "black_box_warning": CF.int64(pandas_nullable=True, isin={0, 1}),
        "chirality": CF.int64(pandas_nullable=True, ge=0),
        "dosed_ingredient": CF.int64(pandas_nullable=True, isin={0, 1}),
        "helm_notation": CF.string(),
        "indication_class": CF.string(),
        "inorganic_flag": CF.int64(pandas_nullable=True, isin={0, 1}),
        "natural_product": CF.int64(pandas_nullable=True, isin={0, 1}),
        "prodrug": CF.int64(pandas_nullable=True, isin={0, 1}),
        "structure_type": CF.string(),
        "therapeutic_flag": CF.int64(pandas_nullable=True, isin={0, 1}),
        # Flattened from molecule_hierarchy
        "molecule_hierarchy__molecule_chembl_id": CF.string(),
        "molecule_hierarchy__parent_chembl_id": CF.string(),
        # Flattened from molecule_structures
        "molecule_structures__canonical_smiles": CF.string(),
        "molecule_structures__molfile": CF.string(),
        "molecule_structures__standard_inchi": CF.string(),
        "molecule_structures__standard_inchi_key": CF.string(
            length=(27, 27), unique=True
        ),
        # Flattened from molecule_properties
        "molecule_properties__alogp": CF.float64(),
        "molecule_properties__aromatic_rings": CF.int64(pandas_nullable=True, ge=0),
        "molecule_properties__cx_logd": CF.float64(),
        "molecule_properties__cx_logp": CF.float64(),
        "molecule_properties__cx_most_apka": CF.float64(),
        "molecule_properties__cx_most_bpka": CF.float64(),
        "molecule_properties__full_molformula": CF.string(),
        "molecule_properties__full_mwt": CF.float64(ge=0),
        "molecule_properties__hba": CF.int64(pandas_nullable=True, ge=0),
        "molecule_properties__hba_lipinski": CF.int64(pandas_nullable=True, ge=0),
        "molecule_properties__hbd": CF.int64(pandas_nullable=True, ge=0),
        "molecule_properties__hbd_lipinski": CF.int64(pandas_nullable=True, ge=0),
        "molecule_properties__heavy_atoms": CF.int64(pandas_nullable=True, ge=0),
        "molecule_properties__molecular_species": CF.string(),
        "molecule_properties__mw_freebase": CF.float64(ge=0),
        "molecule_properties__mw_monoisotopic": CF.float64(ge=0),
        "molecule_properties__num_lipinski_ro5_violations": CF.int64(pandas_nullable=True, ge=0),
        "molecule_properties__num_ro5_violations": CF.int64(pandas_nullable=True, ge=0),
        "molecule_properties__psa": CF.float64(ge=0),
        "molecule_properties__qed_weighted": CF.float64(ge=0),
        "molecule_properties__ro3_pass": CF.int64(pandas_nullable=True, isin={0, 1}),
        "molecule_properties__rtb": CF.int64(pandas_nullable=True, ge=0),
        # Serialized arrays
        "atc_classifications": CF.string(),
        "cross_references__flat": CF.string(),
        "molecule_synonyms__flat": CF.string(),
        # Version fields
        "_chembl_db_version": CF.string(nullable=False),
        "_api_version": CF.string(nullable=False),
        "hash_row": CF.string(length=(64, 64), nullable=False),
        "hash_business_key": CF.string(length=(64, 64)),
        "load_meta_id": CF.uuid(nullable=False),
    },
    version=SCHEMA_VERSION,
    name="TestItemSchema",
    strict=True,
    column_order=COLUMN_ORDER,
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TestItemSchema"]
