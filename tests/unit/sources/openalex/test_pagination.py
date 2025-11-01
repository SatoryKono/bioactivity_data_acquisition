"""Tests for OpenAlex cursor pagination helper."""

from __future__ import annotations

from unittest.mock import MagicMock

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.sources.openalex.pagination import CursorPaginator


def test_cursor_paginator_fetches_all_pages() -> None:
    client = MagicMock(spec=UnifiedAPIClient)
    client.request_json.side_effect = [
        {"results": [{"id": "W1"}, {"id": "W2"}], "meta": {"next_cursor": "abc"}},
        {"results": [{"id": "W3"}], "meta": {"next_cursor": None}},
    ]

    paginator = CursorPaginator(client, page_size=2)
    results = paginator.fetch_all("/works")

    assert [row["id"] for row in results] == ["W1", "W2", "W3"]
    assert client.request_json.call_count == 2


def test_cursor_paginator_deduplicates_by_unique_key() -> None:
    client = MagicMock(spec=UnifiedAPIClient)
    client.request_json.return_value = {
        "results": [{"id": "W1"}, {"id": "W1"}],
        "meta": {"next_cursor": None},
    }

    paginator = CursorPaginator(client)
    results = paginator.fetch_all("/works")

    assert len(results) == 1


def test_cursor_paginator_respects_page_limit() -> None:
    client = MagicMock(spec=UnifiedAPIClient)
    client.request_json.return_value = {"results": [{"id": "W1"}], "meta": {"next_cursor": "abc"}}

    paginator = CursorPaginator(client)
    results = paginator.fetch_all("/works", max_pages=1)

    assert len(results) == 1
    # The second page should not be requested because of the guard.
    client.request_json.assert_called_once_with("/works", params={"per_page": 100})
