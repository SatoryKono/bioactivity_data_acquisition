"""Property-based tests for unified pagination strategies."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import hypothesis.strategies as st
from hypothesis import given

from bioetl.core.pagination import (
    CursorPaginationStrategy,
    CursorRequest,
    PageNumberPaginationStrategy,
    PageNumberRequest,
)


class RecordingClient:
    """Stub :class:`UnifiedAPIClient` recording all request metadata."""

    def __init__(self, json_responses: list[Mapping[str, Any]]) -> None:
        self._json_responses = json_responses
        self.json_calls: list[tuple[str, dict[str, Any]]] = []

    def request_json(self, path: str, *, params: Mapping[str, Any] | None = None) -> Any:
        index = len(self.json_calls)
        self.json_calls.append((path, dict(params or {})))
        if index < len(self._json_responses):
            return self._json_responses[index]
        return {}


def _cursor_response(token: str | None, batch: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    return {
        "results": batch,
        "meta": {"next": token or ""},
    }


@st.composite
def cursor_scenarios(draw: st.DrawFn) -> tuple[list[str], list[Mapping[str, Any]]]:
    tokens = draw(
        st.lists(
            st.text(min_size=1).filter(lambda value: value.strip() == value),
            min_size=1,
            max_size=5,
            unique=True,
        )
    )
    responses: list[Mapping[str, Any]] = []
    for index, token in enumerate(tokens):
        next_token = tokens[index + 1] if index + 1 < len(tokens) else None
        batch = [{"id": f"{token}-{index}"}]
        responses.append(_cursor_response(next_token, batch))
    return tokens, responses


@given(cursor_scenarios())
def test_cursor_request_uses_monotonic_tokens(scenario: tuple[list[str], list[Mapping[str, Any]]]) -> None:
    tokens, responses = scenario
    client = RecordingClient(responses)
    strategy = CursorPaginationStrategy(client, cursor_param="cursor", page_size_param="size")

    def parse_items(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        if isinstance(payload, Mapping):
            raw_items = payload.get("results")
            if isinstance(raw_items, list):
                return [item for item in raw_items if isinstance(item, Mapping)]
        return []

    def extract_cursor(payload: Mapping[str, Any]) -> str | None:
        if isinstance(payload, Mapping):
            meta = payload.get("meta")
            if isinstance(meta, Mapping):
                next_token = meta.get("next")
                if isinstance(next_token, str) and next_token.strip():
                    return next_token
        return None

    request = CursorRequest(
        path="/cursor",
        page_size=10,
        unique_key="id",
        parse_items=parse_items,
        extract_cursor=extract_cursor,
    )

    collected = strategy.paginate(request)

    # Cursor tokens must be consumed strictly in the order provided.
    recorded_tokens = [call[1].get("cursor") for call in client.json_calls[1:]]
    expected_tokens: list[str] = []
    for response in responses[:-1]:
        meta = response.get("meta")
        if isinstance(meta, Mapping):
            token = meta.get("next")
            if isinstance(token, str) and token.strip():
                expected_tokens.append(token)
    assert recorded_tokens == expected_tokens

    # Tokens must not repeat when traversing the cursor chain.
    assert len(recorded_tokens) == len(set(recorded_tokens))

    # The first page must not include a cursor token.
    if client.json_calls:
        assert "cursor" not in client.json_calls[0][1]

    # The number of cursor transitions should match the available batches.
    assert len(collected) == len(tokens)


@st.composite
def page_number_scenarios(
    draw: st.DrawFn,
) -> tuple[int, int, list[int], bool]:
    start_page = draw(st.integers(min_value=1, max_value=5))
    page_size = draw(st.integers(min_value=1, max_value=5))
    page_count = draw(st.integers(min_value=1, max_value=5))

    counts: list[int] = [page_size] * max(0, page_count - 1)

    if page_size == 1:
        last_full = True
        counts.append(1)
    else:
        last_full = draw(st.booleans())
        if last_full:
            counts.append(page_size)
        else:
            counts.append(draw(st.integers(min_value=1, max_value=page_size - 1)))

    return start_page, page_size, counts, last_full


@given(page_number_scenarios())
def test_page_number_request_respects_boundaries(
    scenario: tuple[int, int, list[int], bool]
) -> None:
    start_page, page_size, counts, last_full = scenario
    responses: list[Mapping[str, Any]] = []
    identifier = 0
    for count in counts:
        batch = [{"id": identifier + idx} for idx in range(count)]
        identifier += count
        responses.append({"results": batch})
    if last_full:
        responses.append({"results": []})

    client = RecordingClient(responses)
    strategy = PageNumberPaginationStrategy(
        client,
        page_param="page",
        page_size_param="size",
        offset_param="offset",
        limit_param="limit",
    )

    def parser(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        if isinstance(payload, Mapping):
            raw = payload.get("results")
            if isinstance(raw, list):
                return [item for item in raw if isinstance(item, Mapping)]
        return []

    request = PageNumberRequest(
        path="/page",
        page_size=page_size,
        unique_key="id",
        parser=parser,
        start_page=start_page,
    )

    collected = strategy.paginate(request)

    # Ensure every page between ``start_page`` and the final request was visited.
    expected_calls = len(counts) + (1 if last_full else 0)
    assert len(client.json_calls) == expected_calls

    recorded_pages = [call[1]["page"] for call in client.json_calls]
    assert recorded_pages == [start_page + index for index in range(len(recorded_pages))]

    # Offsets must advance according to the configured page size.
    recorded_offsets = [call[1].get("offset") for call in client.json_calls]
    assert recorded_offsets == [max(0, (page - 1) * page_size) for page in recorded_pages]

    # All generated identifiers should be collected without duplication.
    assert len(collected) == sum(counts)
    assert {item["id"] for item in collected} == set(range(identifier))
