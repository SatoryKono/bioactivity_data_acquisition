"""Tests for Crossref cursor pagination utilities."""

from __future__ import annotations

from unittest.mock import MagicMock

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.sources.crossref.pagination import CursorPaginator


def test_cursor_paginator_fetches_multiple_pages() -> None:
    """Cursor paginator should iterate until the API stops returning a cursor."""

    client = MagicMock(spec=UnifiedAPIClient)
    client.request_json.side_effect = [
        {
            "message": {
                "items": [{"DOI": f"10.1000/test.{i}"} for i in range(3)],
                "next-cursor": "cursor-1",
            }
        },
        {
            "message": {
                "items": [{"DOI": f"10.1000/test.{i}"} for i in range(3, 5)],
                "next-cursor": None,
            }
        },
    ]

    paginator = CursorPaginator(client, page_size=3)
    items = paginator.fetch_all("/works")

    assert [item["DOI"] for item in items] == [
        "10.1000/test.0",
        "10.1000/test.1",
        "10.1000/test.2",
        "10.1000/test.3",
        "10.1000/test.4",
    ]
    assert client.request_json.call_count == 2


def test_cursor_paginator_deduplicates_by_unique_key() -> None:
    """Paginator should drop duplicate items based on the configured key."""

    client = MagicMock(spec=UnifiedAPIClient)
    client.request_json.return_value = {
        "message": {
            "items": [
                {"DOI": "10.1000/duplicate", "title": "First"},
                {"DOI": "10.1000/duplicate", "title": "Second"},
            ],
            "next-cursor": None,
        }
    }

    paginator = CursorPaginator(client, page_size=2)
    items = paginator.fetch_all("/works")

    assert len(items) == 1
    assert items[0]["title"] == "First"
