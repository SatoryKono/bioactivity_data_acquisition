"""Pandera schema for document pipeline inputs."""

import pandera as pa
from pandera.typing import Series


class DocumentInputSchema(pa.DataFrameModel):
    """Validate input identifiers for the document pipeline."""

    document_chembl_id: Series[str] = pa.Field(
        regex=r"^CHEMBL\d+$",
        nullable=False,
        unique=True,
        description="Валидный идентификатор документа ChEMBL",
    )

    class Config:
        strict = True
        ordered = True
        coerce = True

