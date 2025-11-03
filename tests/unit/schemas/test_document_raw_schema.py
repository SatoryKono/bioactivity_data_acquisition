from __future__ import annotations

import pandas as pd

from bioetl.schemas.document import DocumentRawSchema


def test_document_raw_schema_accepts_shuffled_columns() -> None:
    schema = DocumentRawSchema.to_schema()
    expected_order = list(schema.columns.keys())

    # Provide columns in a deliberately shuffled order and include one extra
    # field to mimic the CSV exports that triggered the regression on Windows.
    shuffled_columns = [
        "abstract",
        "document_chembl_id",
        "authors",
        "classification",
        "doi",
        "first_page",
        "source",
        "journal",
        "volume",
        "year",
        "issue",
        "month",
        "document_contains_external_links",
        "is_experimental_doc",
        "pubmed_id",
        "title",
        "last_page",
        "journal_abbrev",
    ]

    extra_column = "contact"
    input_columns = [extra_column, *shuffled_columns]

    df = pd.DataFrame(
        [
            {
                "abstract": "Example abstract",
                "document_chembl_id": "CHEMBL9999999",
                "authors": "Doe J",
                "classification": None,
                "document_contains_external_links": None,
                "doi": "10.1000/example",
                "first_page": None,
                "is_experimental_doc": None,
                "issue": None,
                "journal": None,
                "journal_abbrev": None,
                "last_page": None,
                "month": None,
                "pubmed_id": None,
                "title": "Example title",
                "volume": None,
                "year": None,
                "source": None,
                extra_column: "Dr. Contact",
            }
        ],
        columns=input_columns,
    )

    validated = DocumentRawSchema.validate(df, lazy=True)

    # Pandera should keep the deterministic schema order for the core columns.
    assert list(validated.columns[: len(expected_order)]) == expected_order
    # And the caller-provided extra column remains available after validation.
    assert validated.loc[0, extra_column] == "Dr. Contact"
