from __future__ import annotations

import pandas as pd

from library.io.normalize import (
    coerce_text,
    normalise_doi,
    parse_chembl_response,
    parse_crossref_response,
    parse_pubmed_response,
    to_lc_stripped,
)


def test_to_lc_stripped() -> None:
    assert to_lc_stripped(" Hello ") == "hello"
    assert to_lc_stripped(None) is None
    assert to_lc_stripped(123) == "123"


def test_coerce_text_handles_sequences() -> None:
    assert coerce_text(["Hello", "world"]) == "Hello world"
    assert coerce_text(pd.NA) is None
    assert coerce_text(None) is None


def test_normalise_doi_variants() -> None:
    assert normalise_doi("https://doi.org/10.1000/XYZ") == "10.1000/xyz"
    assert normalise_doi("doi:10.1000/abc ") == "10.1000/abc"
    assert normalise_doi("10.1000/abc?download=1") == "10.1000/abc"
    assert normalise_doi("10.1000/abc#section") == "10.1000/abc"
    assert normalise_doi("not-a-doi") is None


def test_parse_chembl_response() -> None:
    payload = {
        "documents": [
            {
                "document_chembl_id": "CHEMBL1122",
                "doi": "10.1000/xyz",
                "pubmed_id": "12345",
                "title": "Sample title",
            }
        ]
    }
    records = parse_chembl_response(payload)
    assert records[0]["doi_key"] == "10.1000/xyz"
    assert records[0]["pmid"] == "12345"


def test_parse_crossref_response() -> None:
    payload = {
        "message": {
            "DOI": "10.1000/xyz",
            "title": ["Crossref title"],
        }
    }
    records = parse_crossref_response(payload)
    assert records[0]["title"] == "Crossref title"
    assert records[0]["doi_key"] == "10.1000/xyz"


def test_parse_pubmed_response() -> None:
    payload = {
        "result": {
            "uids": ["12345"],
            "12345": {"uid": "12345", "title": "PubMed title", "elocationid": "10.1000/xyz"},
        }
    }
    records = parse_pubmed_response(payload)
    assert records[0]["pmid"] == "12345"
    assert records[0]["doi_key"] == "10.1000/xyz"
