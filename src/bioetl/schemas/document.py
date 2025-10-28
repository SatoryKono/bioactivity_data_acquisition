"""Document schemas for different sources."""

import pandera as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema


class ChEMBLDocumentSchema(BaseSchema):
    """Схема для ChEMBL документов."""

    document_chembl_id: Series[str] = pa.Field(
        nullable=False,
        description="ChEMBL document ID",
    )
    title: Series[str] = pa.Field(nullable=False, description="Document title")
    journal: Series[str] | None = pa.Field(nullable=True, description="Journal name")
    year: Series[int] | None = pa.Field(
        ge=1800, le=2030, nullable=True, description="Publication year"
    )
    doi: Series[str] | None = pa.Field(nullable=True, description="DOI")
    pmid: Series[str] | None = pa.Field(nullable=True, description="PubMed ID")


class PubMedDocumentSchema(BaseSchema):
    """Схема для PubMed документов."""

    pmid: Series[str] = pa.Field(nullable=False, description="PubMed ID")
    title: Series[str] = pa.Field(nullable=False, description="Document title")
    journal: Series[str] | None = pa.Field(nullable=True, description="Journal")
    authors: Series[str] | None = pa.Field(nullable=True, description="Authors")
    abstract: Series[str] | None = pa.Field(nullable=True, description="Abstract")
    doi: Series[str] | None = pa.Field(nullable=True, description="DOI")

