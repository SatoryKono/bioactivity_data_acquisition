"""Tests for HTTP clients."""

from __future__ import annotations

import pytest
import responses

from library.clients import BioactivityClient
from library.config import APIClientConfig, RetrySettings


@responses.activate
def test_client_fetches_paginated_results() -> None:
    config = APIClientConfig(
        name="chembl",
        url="https://example.com/bioactivity",
        pagination_param="page",
        page_size_param="page_size",
        page_size=2,
        max_pages=3,
    )
    responses.add(
        responses.GET,
        "https://example.com/bioactivity",
        json={
            "results": [
                {
                    "compound_id": "CHEMBL1",
                    "target_pref_name": "BRAF",
                    "activity_value": 1.0,
                    "activity_units": "nM",
                },
                {
                    "compound_id": "CHEMBL2",
                    "target_pref_name": "EGFR",
                    "activity_value": 2.0,
                    "activity_units": "nM",
                },
            ]
        },
        status=200,
    )
    responses.add(
        responses.GET,
        "https://example.com/bioactivity",
        json={"results": []},
        status=200,
    )

    client = BioactivityClient(config, retries=RetrySettings(max_tries=1))
    records = client.fetch_records()
    client.close()

    assert len(records) == 2
    assert records[0]["source"] == "chembl"
    assert "retrieved_at" in records[0]
    assert len(responses.calls) == 2


@responses.activate
def test_client_rejects_invalid_payload_structure() -> None:
    config = APIClientConfig(name="chembl", url="https://example.com/bioactivity")
    responses.add(
        responses.GET,
        "https://example.com/bioactivity",
        json={"results": 1},
        status=200,
    )
    client = BioactivityClient(config, retries=RetrySettings(max_tries=1))
    try:
        with pytest.raises(ValueError):
            client.fetch_records()
    finally:
        client.close()


