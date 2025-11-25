from __future__ import annotations

import requests
import responses

from bioetl.core.source import CursorPaginator, HttpSourceClient, RequestBuilderABC, ResponseParserABC


class QueryRequestBuilder(RequestBuilderABC):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def build(self, cursor: str | None = None) -> requests.Request:  # type: ignore[override]
        params: dict[str, str] = {}
        if cursor:
            params["cursor"] = cursor
        return requests.Request("GET", self.base_url, params=params)


class ItemsParser(ResponseParserABC):
    def parse_records(self, response: requests.Response):  # type: ignore[override]
        payload = response.json()
        return payload.get("items", [])

    def get_next_cursor(self, response: requests.Response):  # type: ignore[override]
        payload = response.json()
        return payload.get("next")


@responses.activate
def test_paginates_until_cursor_exhausted():
    base_url = "https://example.com/items"
    responses.add(
        responses.GET,
        base_url,
        json={"items": [{"id": 1}], "next": "abc"},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{base_url}?cursor=abc",
        json={"items": [{"id": 2}], "next": None},
        status=200,
    )

    client = HttpSourceClient(max_retries=2, backoff_factor=0.01)
    builder = QueryRequestBuilder(base_url)
    parser = ItemsParser()
    paginator = CursorPaginator()

    records = list(client.fetch_records(builder, paginator, parser))

    assert records == [{"id": 1}, {"id": 2}]
    assert len(responses.calls) == 2


@responses.activate
def test_retries_on_transient_error_and_recovers():
    base_url = "https://example.com/items"
    responses.add(responses.GET, base_url, status=500)
    responses.add(responses.GET, base_url, json={"items": [], "next": None}, status=200)

    client = HttpSourceClient(max_retries=3, backoff_factor=0.01)
    builder = QueryRequestBuilder(base_url)
    parser = ItemsParser()
    paginator = CursorPaginator()

    records = list(client.fetch_records(builder, paginator, parser))

    assert records == []
    assert len(responses.calls) == 2
