"""Pandera schemas for document input data."""

from __future__ import annotations

import importlib.util

from pandera.typing import Series

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa


class DocumentInputSchema(pa.DataFrameModel):
    """Schema for input document data from ChEMBL CSV files."""

    # Required fields
    document_chembl_id: Series[str] = pa.Field(description="ChEMBL document identifier")
    title: Series[str] = pa.Field(description="Document title")
    
    # Optional fields (matching actual CSV columns)
    doi: Series[str] = pa.Field(nullable=True, description="Digital Object Identifier")
    pubmed_id: Series[str] = pa.Field(nullable=True, description="PubMed identifier")
    journal: Series[str] = pa.Field(nullable=True, description="Journal name")
    year: Series[float] = pa.Field(nullable=True, description="Publication year")
    
    # Legacy fields (matching actual CSV columns)
    abstract: Series[str] = pa.Field(nullable=True, description="Document abstract")
    authors: Series[str] = pa.Field(nullable=True, description="Document authors")
    classification: Series[float] = pa.Field(nullable=True, description="Document classification")
    document_contains_external_links: Series[bool] = pa.Field(nullable=True, description="Contains external links")
    first_page: Series[int] = pa.Field(nullable=True, description="First page number")
    is_experimental_doc: Series[bool] = pa.Field(nullable=True, description="Is experimental document")
    issue: Series[float] = pa.Field(nullable=True, description="Journal issue number")
    last_page: Series[float] = pa.Field(nullable=True, description="Last page number")
    month: Series[int] = pa.Field(nullable=True, description="Publication month")
    volume: Series[float] = pa.Field(nullable=True, description="Journal volume")

    class Config:
        strict = True
        coerce = True


__all__ = ["DocumentInputSchema"]
