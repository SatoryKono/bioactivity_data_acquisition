"""ChEMBL-only Document schema with strict Pandera validation."""

from pandera import Check, Column, DataFrameSchema


DocumentSchema = DataFrameSchema(
    {
        "document_id": Column(str, nullable=False),
        "pubmed_id": Column(int, nullable=True, coerce=True),
        "doi": Column(str, nullable=True),
        "journal": Column(str, nullable=True),
        "year": Column(int, nullable=True, checks=Check.ge(1900) & Check.le(2100)),
        "title": Column(str, nullable=True),
        "authors": Column(str, nullable=True),
    },
    strict=True,
    ordered=True,
)
