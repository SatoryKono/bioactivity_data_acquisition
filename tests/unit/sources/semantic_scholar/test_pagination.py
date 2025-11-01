"""Tests for the Semantic Scholar pagination helpers."""

from __future__ import annotations

from unittest.mock import Mock

from bioetl.sources.semantic_scholar.pagination import OffsetPaginator


def test_fetch_all_accumulates_pages() -> None:
    """OffsetPaginator should iterate through offsets until exhaustion."""

    client = Mock()
    client.request_json.side_effect = [
        {"data": [{"id": 1}, {"id": 2}]},
        {"data": [{"id": 3}]},
        {"data": []},
    ]

    paginator = OffsetPaginator(client=client, page_size=2)
    results = paginator.fetch_all("/paper/search", params={"query": "chemistry"})

    assert [row["id"] for row in results] == [1, 2, 3]
    assert client.request_json.call_count == 2

    first_call = client.request_json.call_args_list[0]
    assert first_call.kwargs["params"]["offset"] == 0
    second_call = client.request_json.call_args_list[1]
    assert second_call.kwargs["params"]["offset"] == 2


def test_fetch_all_respects_max_items() -> None:
    """The paginator truncates the result set when ``max_items`` is reached."""

    client = Mock()
    client.request_json.side_effect = [
        {"data": [{"id": 1}, {"id": 2}]},
        {"data": [{"id": 3}, {"id": 4}]},
    ]

    paginator = OffsetPaginator(client=client, page_size=2)
    results = paginator.fetch_all("/paper/search", max_items=3)

    assert [row["id"] for row in results] == [1, 2, 3]
    assert client.request_json.call_count == 2

    first_call = client.request_json.call_args_list[0]
    assert first_call.kwargs["params"]["limit"] == 2
    second_call = client.request_json.call_args_list[1]
    assert second_call.kwargs["params"]["offset"] == 2
