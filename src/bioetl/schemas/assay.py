"""Assay schema for ChEMBL data."""

import pandera as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema


class AssaySchema(BaseSchema):
    """Schema for ChEMBL assay data."""

    assay_chembl_id: Series[str] = pa.Field(
        nullable=False,
        description="ChEMBL assay identifier",
    )
    assay_type: Series[str] | None = pa.Field(nullable=True, description="Assay type")
    description: Series[str] | None = pa.Field(nullable=True, description="Assay description")
    target_chembl_id: Series[str] | None = pa.Field(nullable=True, description="Target ChEMBL ID")
    confidence_score: Series[int] | None = pa.Field(
        ge=0, le=9, nullable=True, description="Confidence score (0-9)"
    )

