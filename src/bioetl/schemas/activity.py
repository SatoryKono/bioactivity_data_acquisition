"""Pandera schemas for Activity data."""

import pandera as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema


class ActivitySchema(BaseSchema):
    """Schema for ChEMBL Activity data."""

    # ChEMBL IDs
    activity_id: Series[int] = pa.Field(nullable=False, ge=1)
    molecule_chembl_id: Series[str] = pa.Field(nullable=False)
    assay_chembl_id: Series[str] = pa.Field(nullable=True)
    target_chembl_id: Series[str] = pa.Field(nullable=True)
    document_chembl_id: Series[str] = pa.Field(nullable=True)

    # Activity measures
    standard_type: Series[str] = pa.Field(nullable=True)  # IC50, Ki, EC50, etc.
    standard_relation: Series[str] = pa.Field(nullable=True)  # =, >, <, >=, <=
    standard_value: Series[float] = pa.Field(nullable=True, ge=0)
    standard_units: Series[str] = pa.Field(nullable=True)  # nM, µM, mM
    pchembl_value: Series[float] = pa.Field(nullable=True, ge=0)

    # BAO annotations
    bao_endpoint: Series[str] = pa.Field(nullable=True)
    bao_format: Series[str] = pa.Field(nullable=True)
    bao_label: Series[str] = pa.Field(nullable=True)

    # Molecular properties
    canonical_smiles: Series[str] = pa.Field(nullable=True)

    # Target information
    target_organism: Series[str] = pa.Field(nullable=True)
    target_tax_id: Series[int] = pa.Field(nullable=True, ge=1)

    # Validity
    data_validity_comment: Series[str] = pa.Field(nullable=True)

    # Extended properties
    activity_properties: Series[str] = pa.Field(nullable=True)  # JSON string

    class Config:
        strict = True
        coerce = True
        ordered = False

