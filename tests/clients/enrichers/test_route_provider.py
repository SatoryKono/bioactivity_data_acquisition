from __future__ import annotations

from typing import Any

import pytest

from bioetl.clients.providers import CrossrefClient, PubChemClient


class DummyApiClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def fetch_one(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "fetch_one",
                path,
                params or {},
                headers,
                timeout_sec,
                max_retries,
            )
        )
        return {"results": [{"path": path, "params": params}]}

    def fetch_batch(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append(
            (
                "fetch_batch",
                path,
                params or {},
                headers,
                page_key,
                next_key,
                page_param,
                timeout_sec,
                max_retries,
            )
        )
        return [
            {
                "results": [
                    {
                        "path": path,
                        "params": params,
                        "page_key": page_key,
                        "next_key": next_key,
                        "page_param": page_param,
                    }
                ]
            }
        ]


@pytest.mark.parametrize(
    "client_cls, fetch_value, search_value, expected_fetch_path, expected_search_path, query_param",
    [
        (
            CrossrefClient,
            "10.1000/xyz",
            "test query",
            "/works/10.1000/xyz",
            "/works",
            "query",
        ),
        (
            PubChemClient,
            "2244",
            "C1=CC=CC=C1",
            "/compound/2244",
            "/compound/search",
            "smiles",
        ),
    ],
)
def test_route_provider_builds_paths_and_params(
    client_cls,
    fetch_value,
    search_value,
    expected_fetch_path,
    expected_search_path,
    query_param,
):
    transport = DummyApiClient()
    client = client_cls(transport)

    list(client.fetch_one(fetch_value, params={"foo": "bar"}))
    list(client.fetch_batch(search_value, params={"page": "2"}))

    fetch_call = transport.calls[0]
    search_call = transport.calls[1]

    assert fetch_call[1] == expected_fetch_path
    assert fetch_call[2] == {"foo": "bar"}
    assert fetch_call[3] is None  # headers

    assert search_call[1] == expected_search_path
    assert search_call[2] == {query_param: search_value, "page": "2"}
    assert search_call[3] is None  # headers
    assert search_call[4] == "results"


@pytest.mark.parametrize(
    "client_cls, alias, target",
    [
        (CrossrefClient, "fetch", "fetch_one"),
        (CrossrefClient, "search", "fetch_batch"),
        (PubChemClient, "fetch_by_cid", "fetch_one"),
        (PubChemClient, "search_by_smiles", "fetch_batch"),
    ],
)
def test_deprecated_aliases_emit_warning(client_cls, alias, target):
    transport = DummyApiClient()
    client = client_cls(transport)
    method = getattr(client, alias)

    with pytest.warns(DeprecationWarning) as captured:
        list(method("value"))

    assert target in str(captured[0].message)
    assert transport.calls, "Underlying method should be invoked"
