"""Contract tests for Chembl activity parser and normalizer."""

from __future__ import annotations

import pytest

from bioetl.schemas import get_schema
from bioetl.sources.chembl.activity.normalizer import ChemblActivityNormalizer
from bioetl.sources.chembl.activity.parser import ChemblActivityParser

_SCHEMA_IDENTIFIER = "bioetl.schemas.chembl_activity_schema.ActivitySchema"


@pytest.fixture(name="sample_raw_payload")
def sample_raw_payload_fixture() -> dict[str, object]:
    """Provide a representative raw response from the ChEMBL API."""

    return {
        "page_meta": {"offset": 0, "limit": 2, "count": 2, "next": None},
        "activities": [
            {
                "activity_id": "100",
                "activity_comment": "first",
                "document_chembl_id": "CHEMBL112233",
            },
            {
                "activity_id": "200",
                "activity_comment": None,
            },
        ],
    }


def test_activity_parser_produces_iterable_dicts(sample_raw_payload: dict[str, object]) -> None:
    parser = ChemblActivityParser()

    parsed = parser.parse(sample_raw_payload)

    records = list(parsed)
    assert len(records) == 2
    assert all(isinstance(item, dict) for item in records)
    assert records[0]["activity_id"] == "100"


def test_activity_parser_handles_none_payload() -> None:
    parser = ChemblActivityParser()

    assert list(parser.parse(None)) == []
    assert list(parser.parse({"activities": []})) == []


def test_activity_normalizer_emits_schema_columns(sample_raw_payload: dict[str, object]) -> None:
    descriptor = get_schema(_SCHEMA_IDENTIFIER)
    parser = ChemblActivityParser()
    normalizer = ChemblActivityNormalizer()

    parsed_record = next(iter(parser.parse(sample_raw_payload)))
    normalized = normalizer.normalize(parsed_record)

    assert set(normalized.keys()) == set(descriptor.schema.columns.keys())
    assert normalized["activity_id"] == "100"


def test_activity_normalizer_handles_missing_record() -> None:
    normalizer = ChemblActivityNormalizer()

    normalized = normalizer.normalize(None)

    assert normalized["activity_id"] is None
