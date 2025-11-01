"""Unit tests for the PubChem parser utilities."""

from __future__ import annotations

from bioetl.sources.pubchem.parser.pubchem_parser import PubChemParser


def test_parse_cid_response_returns_first_entry() -> None:
    payload = {"IdentifierList": {"CID": [12345, 67890]}}
    assert PubChemParser.parse_cid_response(payload) == 12345


def test_parse_cid_response_handles_missing_fields() -> None:
    assert PubChemParser.parse_cid_response({}) is None
    assert PubChemParser.parse_cid_response({"IdentifierList": {"CID": []}}) is None


def test_parse_properties_response_filters_non_dict_entries() -> None:
    payload = {
        "PropertyTable": {
            "Properties": [
                {"CID": 1, "SMILES": "C"},
                "invalid",
                {"CID": 2, "SMILES": "CC"},
            ]
        }
    }
    records = PubChemParser.parse_properties_response(payload)
    assert records == [{"CID": 1, "SMILES": "C"}, {"CID": 2, "SMILES": "CC"}]


def test_extract_cids_from_identifier_list_returns_all_numeric_values() -> None:
    payload = {"IdentifierList": {"CID": ["1", "two", 3]}}
    assert PubChemParser.extract_cids_from_identifier_list(payload) == [1, 3]
