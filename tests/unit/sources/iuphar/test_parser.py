"""Parsing helpers for the IUPHAR source."""

from __future__ import annotations

from bioetl.sources.iuphar.parser import coerce_items, parse_api_response


def test_coerce_items_extracts_results_from_mapping() -> None:
    """Nested mappings should be flattened into a list of dictionaries."""

    payload = {"results": [{"targetId": 1}, {"targetId": 2}]}

    items = coerce_items(payload, unique_key="targetId")

    assert len(items) == 2
    assert items[0]["targetId"] == 1


def test_parse_api_response_accepts_single_record() -> None:
    """A single mapping with the unique key should be returned as a list."""

    payload = {"targetId": 42, "name": "Example"}

    items = parse_api_response(payload, unique_key="targetId")

    assert len(items) == 1
    assert items[0]["name"] == "Example"
