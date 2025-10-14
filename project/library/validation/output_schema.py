"""Pandera schema for normalized publication records."""

from __future__ import annotations

import pandera as pa

OUTPUT_SCHEMA = pa.DataFrameSchema(
    {
        "chembl.document_chembl_id": pa.Column(pa.String, nullable=False),
        "doi_key": pa.Column(pa.String, nullable=True),
        "pmid": pa.Column(pa.String, nullable=True),
        "chembl.title": pa.Column(pa.String, nullable=True),
        "chembl.doi": pa.Column(pa.String, nullable=True),
        "chembl.pmid": pa.Column(pa.String, nullable=True),
        "chembl.journal": pa.Column(pa.String, nullable=True),
        "chembl.year": pa.Column(pa.Float, nullable=True),
        "pubmed.title": pa.Column(pa.String, nullable=True),
        "pubmed.journal": pa.Column(pa.String, nullable=True),
        "pubmed.pub_date": pa.Column(pa.String, nullable=True),
        "pubmed.doi": pa.Column(pa.String, nullable=True),
        "semscholar.title": pa.Column(pa.String, nullable=True),
        "semscholar.year": pa.Column(pa.String, nullable=True),
        "semscholar.paper_id": pa.Column(pa.String, nullable=True),
        "crossref.title": pa.Column(pa.String, nullable=True),
        "crossref.issued": pa.Column(pa.String, nullable=True),
        "crossref.publisher": pa.Column(pa.String, nullable=True),
        "crossref.type": pa.Column(pa.String, nullable=True),
        "openalex.title": pa.Column(pa.String, nullable=True),
        "openalex.publication_year": pa.Column(pa.Float, nullable=True),
        "openalex.doi": pa.Column(pa.String, nullable=True),
    },
    strict=True,
    name="Publications",
)
