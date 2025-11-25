from __future__ import annotations

from unittest.mock import MagicMock, call

from bioetl.clients.common import NextLinkPagination, PageParamPagination


def test_next_link_pagination_traverses_next_links_and_params() -> None:
    api_client = MagicMock()
    api_client.get_json.side_effect = [
        {"results": [{"id": 1}, {"id": 2}], "next": "/entities?page=2"},
        {"results": [{"id": 3}], "next": None},
    ]

    logger = MagicMock()
    strategy = NextLinkPagination()

    result = list(strategy.paginate(api_client, "/entities", params={"limit": 2}, logger=logger))

    assert result == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert api_client.get_json.call_args_list == [
        call("/entities", params={"limit": 2}),
        call("/entities?page=2", params=None),
    ]
    assert logger.info.call_args_list == [
        call("api_call", path="/entities"),
        call("api_call", path="/entities?page=2"),
    ]


def test_next_link_pagination_yields_payload_without_results() -> None:
    api_client = MagicMock()
    api_client.get_json.return_value = {"value": 42}

    strategy = NextLinkPagination()

    result = list(strategy.paginate(api_client, "/entities"))

    assert result == [{"value": 42}]
    api_client.get_json.assert_called_once_with("/entities", params=None)


def test_page_param_pagination_uses_paginate_json_and_flattens_results() -> None:
    api_client = MagicMock()
    api_client.paginate_json.return_value = iter(
        [
            {"results": [{"id": 1}]},
            {"results": [{"id": 2}, {"id": 3}]},
        ]
    )

    strategy = PageParamPagination(page_param="page")

    result = list(strategy.paginate(api_client, "/entities", params={"limit": 50, "foo": "bar"}))

    assert result == [{"id": 1}, {"id": 2}, {"id": 3}]
    api_client.paginate_json.assert_called_once_with(
        "/entities",
        params={"limit": 50, "foo": "bar"},
        page_key="results",
        next_key="next",
        page_param="page",
    )


def test_page_param_pagination_falls_back_without_results() -> None:
    api_client = MagicMock()
    api_client.paginate_json.return_value = iter([{"value": "fallback"}])

    strategy = PageParamPagination()

    result = list(strategy.paginate(api_client, "/entities"))

    assert result == [{"value": "fallback"}]
    api_client.paginate_json.assert_called_once_with(
        "/entities",
        params=None,
        page_key="results",
        next_key="next",
        page_param="page",
    )
