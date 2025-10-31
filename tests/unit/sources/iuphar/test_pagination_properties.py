from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from hypothesis import given, strategies as st

from bioetl.sources.iuphar.pagination import PageNumberPaginator


class FakeClient:
    """Fake UnifiedAPIClient that replays a predetermined sequence of pages."""

    def __init__(self, pages: list[Any], *, path: str) -> None:
        self._pages = list(pages)
        self._path = path
        self.requests: list[dict[str, Any]] = []
        self._call_index = 0

    def request_json(self, path: str, params: Mapping[str, Any]) -> Any:  # pragma: no cover - exercised via tests
        assert path == self._path
        self.requests.append(dict(params))
        if self._call_index < len(self._pages):
            payload = self._pages[self._call_index]
        else:
            payload = []
        self._call_index += 1
        return payload


class RecordingParser:
    def __init__(self) -> None:
        self.payloads: list[Any] = []

    def __call__(self, payload: Any) -> list[Any]:  # pragma: no cover - exercised via tests
        self.payloads.append(payload)
        if isinstance(payload, list):
            return list(payload)
        return [payload]


def _simulate_expected_collection(pages: list[Any], page_size: int) -> tuple[list[dict[str, Any]], int]:
    seen: set[Any] = set()
    collected: list[dict[str, Any]] = []
    requests = 0
    page_index = 0

    while True:
        requests += 1
        payload = pages[page_index] if page_index < len(pages) else []
        page_index += 1
        items = payload if isinstance(payload, list) else [payload]

        filtered: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            key = item.get("id")
            if key is not None and key in seen:
                continue
            if key is not None:
                seen.add(key)
            filtered.append(dict(item))

        if not filtered:
            break

        collected.extend(filtered)
        if len(filtered) < page_size:
            break

    return collected, requests


PARAM_KEYS = ["foo", "bar", "baz", "token"]


@st.composite
def base_params_strategy(draw) -> dict[str, Any]:
    keys = draw(st.lists(st.sampled_from(PARAM_KEYS), unique=True, max_size=len(PARAM_KEYS)))
    params: dict[str, Any] = {}
    for key in keys:
        params[key] = draw(st.integers(min_value=0, max_value=5))
    return params


IDENTIFIER_VALUES = st.one_of(
    st.none(),
    st.integers(min_value=-2, max_value=5),
    st.text(alphabet="abc", min_size=1, max_size=3),
)


@st.composite
def mapping_items(draw) -> dict[str, Any]:
    include_id = draw(st.booleans())
    extra = draw(
        st.dictionaries(
            keys=st.sampled_from(["name", "value", "extra"]),
            values=st.one_of(
                st.text(alphabet="xyz", min_size=0, max_size=3),
                st.integers(min_value=-3, max_value=9),
            ),
            max_size=2,
        )
    )
    result = dict(extra)
    if include_id:
        result["id"] = draw(IDENTIFIER_VALUES)
    return result


ITEMS = st.one_of(
    mapping_items(),
    st.integers(min_value=-5, max_value=5),
    st.text(alphabet="uvw", min_size=0, max_size=3),
    st.booleans(),
    st.none(),
)


@st.composite
def page_payloads(draw) -> Any:
    if draw(st.booleans()):
        return draw(st.lists(ITEMS, min_size=0, max_size=5))
    return draw(ITEMS)


@given(
    page_size=st.integers(min_value=1, max_value=5),
    params=base_params_strategy(),
    pages=st.lists(page_payloads(), min_size=0, max_size=5),
)
def test_fetch_all_property_checks(page_size: int, params: dict[str, Any], pages: list[Any]) -> None:
    path = "/resource"
    client = FakeClient(pages, path=path)
    parser = RecordingParser()
    paginator = PageNumberPaginator(client, page_size=page_size)

    params_snapshot = dict(params)

    result = paginator.fetch_all(path, unique_key="id", params=params, parser=parser)

    expected, expected_requests = _simulate_expected_collection(pages, page_size)

    assert result == expected
    assert params == params_snapshot
    assert len(client.requests) == expected_requests

    ids = [row["id"] for row in result if row.get("id") is not None]
    assert len(ids) == len(set(ids))
    assert all(isinstance(row, dict) for row in result)

    for index, query in enumerate(client.requests, start=1):
        for key, value in params_snapshot.items():
            assert query[key] == value
        assert query["page"] == index
        assert query["offset"] == (index - 1) * page_size
        assert query["limit"] == page_size
        assert query["pageSize"] == page_size


def test_skip_non_mapping_items() -> None:
    path = "/resource"
    pages = [[{"id": 1, "value": "ok"}, "oops", 3.14, {"id": 1, "value": "duplicate"}, {"value": "no id"}]]
    client = FakeClient(pages, path=path)
    parser = RecordingParser()
    paginator = PageNumberPaginator(client, page_size=5)

    result = paginator.fetch_all(path, unique_key="id", params={}, parser=parser)

    assert result == [{"id": 1, "value": "ok"}, {"value": "no id"}]
    assert all(isinstance(item, dict) for item in result)


def test_parser_required() -> None:
    path = "/resource"
    client = FakeClient([], path=path)
    paginator = PageNumberPaginator(client, page_size=1)

    with pytest.raises(ValueError):
        paginator.fetch_all(path, unique_key="id", params={}, parser=None)
