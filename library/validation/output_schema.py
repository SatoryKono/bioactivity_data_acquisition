"""Pandera schema definitions for pipeline outputs."""
from __future__ import annotations

import pandera.pandas as pa


OUTPUT_SCHEMA = pa.DataFrameSchema(
    {
        "document_chembl_id": pa.Column(str, nullable=False, coerce=True),
        "doi_key": pa.Column(str, nullable=True, coerce=True),
        "pmid": pa.Column(str, nullable=True, coerce=True),
        "chembl_title": pa.Column(str, nullable=True, coerce=True),
        "chembl_doi": pa.Column(str, nullable=True, coerce=True),
        "crossref_title": pa.Column(str, nullable=True, coerce=True),
        "pubmed_title": pa.Column(str, nullable=True, coerce=True),
    },
    strict=True,
    coerce=True,
    name="FetchPublicationsOutput",
)
