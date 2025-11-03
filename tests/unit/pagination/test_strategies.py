"""Unit tests for core pagination strategies."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from bioetl.core.pagination import (
    CursorPaginationStrategy,
    OffsetPaginationStrategy,
    PageNumberPaginationStrategy,
    TokenInitialization,
    TokenPaginationStrategy,
)


class StubClient:
    """Minimal stub mimicking :class:`UnifiedAPIClient`."""

    def __init__(self, json_responses: list[Any] | None = None, text_responses: list[str] | None = None) -> None:
        self._json_responses = json_responses or []
        self._text_responses = text_responses or []
        self.json_calls: list[tuple[str, dict[str, Any] | None]] = []
        self.text_calls: list[tuple[str, dict[str, Any] | None]] = []

    def request_json(self, path: str, *, params: Mapping[str, Any] | None = None) -> Any:
        index = len(self.json_calls)
        self.json_calls.append((path, dict(params or {})))
        if index < len(self._json_responses):
            return self._json_responses[index]
        return {}

    def request_text(self, path: str, *, params: Mapping[str, Any] | None = None) -> str:
        index = len(self.text_calls)
        self.text_calls.append((path, dict(params or {})))
        if index < len(self._text_responses):
            return self._text_responses[index]
        return ""


def test_page_number_strategy_collects_unique_items() -> None:
    responses = [
        {"results": [{"id": 1}, {"id": 2}, {"id": 2}]},
        {"results": [{"id": 3}]},
        {"results": []},
    ]
    client = StubClient(json_responses=responses)
    strategy = PageNumberPaginationStrategy(client, page_param="page", page_size_param="pageSize")

    def parser(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        return payload.get("results", []) if isinstance(payload, Mapping) else []

    collected = strategy.collect("/endpoint", page_size=2, unique_key="id", parser=parser)

    assert collected == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert client.json_calls[0][1]["page"] == 1
    assert client.json_calls[1][1]["page"] == 2


def test_offset_strategy_respects_max_items_and_params() -> None:
    responses = [
        {"data": [{"name": "a"}, {"name": "b"}]},
        {"data": [{"name": "c"}]},
    ]
    client = StubClient(json_responses=responses)
    strategy = OffsetPaginationStrategy(client, offset_param="offset", limit_param="limit")

    collected = strategy.collect(
        "/search",
        page_size=2,
        params={"q": "term"},
        max_items=3,
        extract_items=lambda payload: payload.get("data", []) if isinstance(payload, Mapping) else [],
    )

    assert collected == [{"name": "a"}, {"name": "b"}, {"name": "c"}]
    assert client.json_calls[0][1] == {"q": "term", "limit": 2, "offset": 0}
    assert client.json_calls[1][1] == {"q": "term", "limit": 2, "offset": 2}


def test_cursor_strategy_invokes_callbacks() -> None:
    responses = [
        {"results": [{"id": "A"}, {"id": "B"}], "meta": {"next": "cursor-2"}},
        {"results": [], "meta": {"next": ""}},
    ]
    client = StubClient(json_responses=responses)
    strategy = CursorPaginationStrategy(client, page_size_param="size")

    invalid_called = []
    empty_called = []
    limit_called = []

    def parse_items(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        return payload.get("results", []) if isinstance(payload, Mapping) else []

    def extract_cursor(payload: Mapping[str, Any]) -> str | None:
        meta = payload.get("meta")
        if isinstance(meta, Mapping):
            next_cursor = meta.get("next")
            if isinstance(next_cursor, str) and next_cursor:
                return next_cursor
        return None

    def on_invalid(_: int) -> None:
        invalid_called.append(True)

    def on_empty(page_index: int) -> None:
        empty_called.append(page_index)

    def on_limit(page_index: int) -> None:
        limit_called.append(page_index)

    collected = strategy.collect(
        "/cursor",
        page_size=2,
        unique_key="id",
        parse_items=parse_items,
        extract_cursor=extract_cursor,
        on_invalid_payload=on_invalid,
        on_empty_page=on_empty,
        on_page_limit=on_limit,
        max_pages=5,
    )

    assert collected == [{"id": "A"}, {"id": "B"}]
    assert not invalid_called
    assert empty_called == [2]
    assert not limit_called
    assert client.json_calls[0][1]["size"] == 2
    assert client.json_calls[1][1]["cursor"] == "cursor-2"


def test_cursor_strategy_enforces_page_limit() -> None:
    responses = [
        {"results": [{"id": "A"}], "meta": {"next": "cursor-2"}},
        {"results": [{"id": "B"}], "meta": {"next": "cursor-3"}},
    ]
    client = StubClient(json_responses=responses)
    strategy = CursorPaginationStrategy(client, page_size_param="size")

    limit_called: list[int] = []

    def parse(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        return payload.get("results", []) if isinstance(payload, Mapping) else []

    def extract(payload: Mapping[str, Any]) -> str | None:
        meta = payload.get("meta")
        if isinstance(meta, Mapping):
            next_cursor = meta.get("next")
            if isinstance(next_cursor, str) and next_cursor:
                return next_cursor
        return None

    collected = strategy.collect(
        "/cursor",
        page_size=1,
        unique_key="id",
        parse_items=parse,
        extract_cursor=extract,
        max_pages=1,
        on_page_limit=lambda page_index: limit_called.append(page_index),
    )

    assert collected == [{"id": "A"}]
    assert limit_called == [1]


def test_token_strategy_collects_batches() -> None:
    client = StubClient(json_responses=[{"token": "abc", "total": 5}], text_responses=["batch-1", "batch-2", "batch-3"])
    strategy = TokenPaginationStrategy(client)

    def initializer(_) -> TokenInitialization[dict[str, Any]] | None:
        return TokenInitialization(state={"token": "abc"}, total_count=5)

    def fetcher(_, state: Mapping[str, Any], offset: int, chunk_size: int) -> list[Mapping[str, Any]]:
        assert state["token"] == "abc"
        if offset >= 5:
            return []
        end = offset + chunk_size
        return [{"value": idx} for idx in range(offset, end)]

    collected = strategy.collect(initializer=initializer, fetcher=fetcher, batch_size=2)

    assert collected == [
        {"value": 0},
        {"value": 1},
        {"value": 2},
        {"value": 3},
        {"value": 4},
    ]


@pytest.mark.parametrize("strategy_cls", [PageNumberPaginationStrategy, OffsetPaginationStrategy, CursorPaginationStrategy, TokenPaginationStrategy])
def test_strategies_raise_on_invalid_size(strategy_cls: Any) -> None:
    client = StubClient()
    strategy = strategy_cls(client)

    with pytest.raises(ValueError):
        if strategy_cls is PageNumberPaginationStrategy:
            strategy.collect("/", page_size=0, unique_key="id", parser=lambda _: [])  # type: ignore[arg-type]
        elif strategy_cls is OffsetPaginationStrategy:
            strategy.collect("/", page_size=0, extract_items=lambda _: [])  # type: ignore[arg-type]
        elif strategy_cls is CursorPaginationStrategy:
            strategy.collect(
                "/",
                page_size=0,
                unique_key="id",
                parse_items=lambda _: [],
                extract_cursor=lambda _: None,
            )
        else:  # TokenPaginationStrategy
            strategy.collect(initializer=lambda _: None, fetcher=lambda *_: [], batch_size=0)
