"""PubMed schema coverage tests."""

from __future__ import annotations

import pandas as pd

from bioetl.schemas.document import DocumentSchema
from bioetl.sources.pubmed.schema import PubMedNormalizedSchema, PubMedRawSchema


def test_document_schema_includes_pubmed_columns() -> None:
    """Ensure the consolidated document schema exposes PubMed columns."""

    columns = set(DocumentSchema.get_column_order())
    expected = {
        "pubmed_pmid",
        "pubmed_article_title",
        "pubmed_abstract",
        "pubmed_doi",
        "pubmed_mesh_descriptors",
    }

    assert expected.issubset(columns)


def test_pubmed_raw_schema_validates_minimal_payload() -> None:
    """A minimally populated raw DataFrame should satisfy the schema."""

    df = pd.DataFrame(
        {
            "index": [0],
            "hash_row": ["0" * 64],
            "hash_business_key": ["1" * 64],
            "pipeline_version": ["1.0.0"],
            "run_id": ["test"],
            "source_system": ["pubmed"],
            "chembl_release": ["chembl_34"],
            "extracted_at": ["2024-01-01T00:00:00Z"],
            "pmid": pd.Series([1234567], dtype="Int64"),
            "title": ["Sample Title"],
            "abstract": ["Sample Abstract"],
            "journal": ["Sample Journal"],
            "journal_abbrev": ["S. J."],
            "volume": ["10"],
            "issue": ["2"],
            "first_page": ["101"],
            "last_page": ["108"],
            "year": pd.Series([2024], dtype="Int64"),
            "month": pd.Series([5], dtype="Int64"),
            "day": pd.Series([12], dtype="Int64"),
            "issn": [["1234-5678"]],
            "doi": ["10.1000/sample"],
            "authors": [[{"last_name": "Doe", "fore_name": "Jane"}]],
            "mesh_terms": [["Term1", "Term2"]],
            "chemicals": [["Chem1"]],
        }
    )

    validated = PubMedRawSchema.validate(df)
    assert not validated.empty


def test_pubmed_normalized_schema_validates_payload() -> None:
    """Normalized schema should accept enrichment-ready frames."""

    df = pd.DataFrame(
        {
            "index": [0],
            "hash_row": ["0" * 64],
            "hash_business_key": ["1" * 64],
            "pipeline_version": ["1.0.0"],
            "run_id": ["test"],
            "source_system": ["pubmed"],
            "chembl_release": ["chembl_34"],
            "extracted_at": ["2024-01-01T00:00:00Z"],
            "pmid": pd.Series([1234567], dtype="Int64"),
            "pubmed_pmid": pd.Series([1234567], dtype="Int64"),
            "doi_clean": ["10.1000/sample"],
            "pubmed_article_title": ["Normalized Title"],
            "pubmed_abstract": ["Normalized Abstract"],
            "pubmed_journal": ["Normalized Journal"],
            "pubmed_journal_abbrev": ["N. J."],
            "pubmed_year": pd.Series([2024], dtype="Int64"),
            "pubmed_month": pd.Series([5], dtype="Int64"),
            "pubmed_day": pd.Series([12], dtype="Int64"),
            "pubmed_volume": ["10"],
            "pubmed_issue": ["2"],
            "pubmed_first_page": ["101"],
            "pubmed_last_page": ["108"],
            "pubmed_doi": ["10.1000/sample"],
            "pubmed_issn": ["1234-5678"],
            "pubmed_issn_print": ["1234-5678"],
            "pubmed_issn_electronic": ["8765-4321"],
            "pubmed_authors": ["Doe, Jane; Smith, John"],
            "pubmed_mesh_descriptors": ["Term1 | Term2"],
            "pubmed_mesh_qualifiers": ["Qualifier"],
            "pubmed_chemical_list": ["Chem1"],
            "pubmed_doc_type": ["Journal Article"],
            "pubmed_year_completed": pd.Series([2024], dtype="Int64"),
            "pubmed_month_completed": pd.Series([5], dtype="Int64"),
            "pubmed_day_completed": pd.Series([15], dtype="Int64"),
            "pubmed_year_revised": pd.Series([2025], dtype="Int64"),
            "pubmed_month_revised": pd.Series([1], dtype="Int64"),
            "pubmed_day_revised": pd.Series([2], dtype="Int64"),
        }
    )

    validated = PubMedNormalizedSchema.validate(df)
    assert not validated.empty
