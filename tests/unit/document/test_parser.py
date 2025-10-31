"""Tests for the document input parser helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.sources.chembl.document.parser import prepare_document_input_ids


def test_prepare_document_input_ids_filters_invalid_entries() -> None:
    """The parser should normalise identifiers and report rejected rows."""

    frame = pd.DataFrame(
        {
            "document_chembl_id": [
                "chembl1",
                " CHEMBL2 ",
                "CHEMBL2",  # duplicate after normalisation
                "not-an-id",
                None,
                "",
            ]
        }
    )

    valid, rejected = prepare_document_input_ids(frame)

    assert valid == ["CHEMBL1", "CHEMBL2"]
    assert {item["document_chembl_id"]: item["reason"] for item in rejected} == {
        "not-an-id": "invalid_format",
        "": "missing",
    }


def test_prepare_document_input_ids_requires_column() -> None:
    """Missing identifier column should raise a descriptive error."""

    frame = pd.DataFrame({"other": [1, 2, 3]})

    with pytest.raises(ValueError, match="document_chembl_id"):
        prepare_document_input_ids(frame)
