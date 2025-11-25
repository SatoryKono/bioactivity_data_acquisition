from __future__ import annotations

import pandera as pa


_HASH = pa.Check.str_length(min_value=64, max_value=64)


def activity_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "activity_id": pa.Column(pa.Int64, nullable=False, checks=pa.Check.ge(0)),
            "row_subtype": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "row_index": pa.Column(pa.Int64, nullable=False, checks=pa.Check.ge(0)),
            "assay_chembl_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "testitem_chembl_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "molecule_chembl_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "target_chembl_id": pa.Column(pa.String, nullable=True, required=False),
            "document_chembl_id": pa.Column(pa.String, nullable=True, required=False),
            "load_meta_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "hash_business_key": pa.Column(pa.String, nullable=True, required=False, checks=_HASH),
            "hash_row": pa.Column(pa.String, nullable=False, checks=_HASH),
            "activity_properties": pa.Column(pa.Object, nullable=True, required=False),
            "value": pa.Column(pa.Float64, nullable=True, required=False),
            "standard_value": pa.Column(pa.Float64, nullable=True, required=False),
            "standard_units": pa.Column(pa.String, nullable=True, required=False),
            "assay_type": pa.Column(pa.String, nullable=True, required=False),
            "assay_description": pa.Column(pa.String, nullable=True, required=False),
            "assay_organism": pa.Column(pa.String, nullable=True, required=False),
            "assay_tax_id": pa.Column(pa.Int64, nullable=True, required=False),
            "target_pref_name": pa.Column(pa.String, nullable=True, required=False),
            "target_organism": pa.Column(pa.String, nullable=True, required=False),
            "target_tax_id": pa.Column(pa.Int64, nullable=True, required=False),
        },
        coerce=True,
        strict=False,
    )


def assays_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "assay_chembl_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "row_subtype": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "row_index": pa.Column(pa.Int64, nullable=False, checks=pa.Check.ge(0)),
            "load_meta_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "hash_business_key": pa.Column(pa.String, nullable=True, required=False, checks=_HASH),
            "hash_row": pa.Column(pa.String, nullable=False, checks=_HASH),
            "target_chembl_id": pa.Column(pa.String, nullable=True, required=False),
            "document_chembl_id": pa.Column(pa.String, nullable=True, required=False),
            "assay_type": pa.Column(pa.String, nullable=True, required=False),
            "assay_category": pa.Column(pa.String, nullable=True, required=False),
            "assay_organism": pa.Column(pa.String, nullable=True, required=False),
            "assay_tax_id": pa.Column(pa.Int64, nullable=True, required=False),
            "confidence_score": pa.Column(pa.Int64, nullable=True, required=False),
            "confidence_description": pa.Column(pa.String, nullable=True, required=False),
        },
        coerce=True,
        strict=False,
    )


def targets_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "target_chembl_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "load_meta_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "hash_business_key": pa.Column(pa.String, nullable=True, required=False, checks=_HASH),
            "hash_row": pa.Column(pa.String, nullable=False, checks=_HASH),
            "pref_name": pa.Column(pa.String, nullable=True, required=False),
            "target_type": pa.Column(pa.String, nullable=True, required=False),
            "organism": pa.Column(pa.String, nullable=True, required=False),
            "tax_id": pa.Column(pa.Int64, nullable=True, required=False),
            "species_group_flag": pa.Column(pa.Int64, nullable=True, required=False),
            "component_count": pa.Column(pa.Int64, nullable=True, required=False),
        },
        coerce=True,
        strict=False,
    )


def documents_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "document_chembl_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "source": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "hash_business_key": pa.Column(pa.String, nullable=False, checks=_HASH),
            "hash_row": pa.Column(pa.String, nullable=False, checks=_HASH),
            "load_meta_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "journal": pa.Column(pa.String, nullable=True, required=False),
            "year": pa.Column(pa.Int64, nullable=True, required=False),
            "title": pa.Column(pa.String, nullable=True, required=False),
            "abstract": pa.Column(pa.String, nullable=True, required=False),
            "doi": pa.Column(pa.String, nullable=True, required=False),
        },
        coerce=True,
        strict=False,
    )


def testitems_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "molecule_chembl_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "_chembl_db_version": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "_api_version": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "load_meta_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "hash_business_key": pa.Column(pa.String, nullable=True, required=False, checks=_HASH),
            "hash_row": pa.Column(pa.String, nullable=False, checks=_HASH),
            "pref_name": pa.Column(pa.String, nullable=True, required=False),
            "molecule_type": pa.Column(pa.String, nullable=True, required=False),
            "max_phase": pa.Column(pa.Int64, nullable=True, required=False),
            "first_approval": pa.Column(pa.Int64, nullable=True, required=False),
            "availability_type": pa.Column(pa.Int64, nullable=True, required=False),
            "black_box_warning": pa.Column(pa.Int64, nullable=True, required=False),
        },
        coerce=True,
        strict=False,
    )
