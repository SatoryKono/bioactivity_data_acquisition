from __future__ import annotations

from unittest.mock import MagicMock, call

from bioetl.core.http.pagination import NextLinkPagination, PageParamPagination


def test_next_link_pagination_traverses_next_links_and_params() -> None:
    transport = MagicMock()
    transport.get.side_effect = [
        {"results": [{"id": 3}], "next": None},
    ]

    logger = MagicMock()
    strategy = NextLinkPagination()

    initial = {"results": [{"id": 1}, {"id": 2}], "next": "/entities?page=2"}

    pages = strategy.iter_pages(
        initial,
        transport,
        path="/entities",
        params={"limit": 2},
        logger=logger,
    )

    flattened = [item for payload in pages for item in payload.get("results", [])]

    assert flattened == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert transport.get.call_args_list == [call("/entities?page=2", params=None)]
    assert logger.info.call_args_list == [call("api_call", path="/entities?page=2")]


def test_next_link_pagination_yields_payload_without_results() -> None:
    transport = MagicMock()
    initial = {"value": 42}

    strategy = NextLinkPagination()

    pages = list(
        strategy.iter_pages(
            initial,
            transport,
            path="/entities",
            params={},
        )
    )

    assert pages == [initial]
    transport.get.assert_not_called()


def test_page_param_pagination_iterates_until_page_empty() -> None:
    transport = MagicMock()
    transport.get.side_effect = [
        {"results": [{"id": 2}, {"id": 3}], "next": None},
        {"results": []},
    ]

    strategy = PageParamPagination(page_param="page")

    pages = list(
        strategy.iter_pages(
            {"results": [{"id": 1}], "next": None},
            transport,
            path="/entities",
            params={"limit": 50, "foo": "bar"},
        )
    )

    flattened = [item for payload in pages for item in payload.get("results", [])]

    assert flattened == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert transport.get.call_args_list == [
        call("/entities", params={"limit": 50, "foo": "bar", "page": 2}),
        call("/entities", params={"limit": 50, "foo": "bar", "page": 3}),
    ]


def test_page_param_pagination_respects_next_link_over_page_iteration() -> None:
    transport = MagicMock()
    transport.get.return_value = {"results": [{"id": 3}]}

    strategy = PageParamPagination()

    pages = list(
        strategy.iter_pages(
            {"results": [{"id": 1}], "next": "/entities?page=2"},
            transport,
            path="/entities",
            params={},
        )
    )

    flattened = [item for payload in pages for item in payload.get("results", [])]

    assert flattened == [{"id": 1}, {"id": 3}]
    transport.get.assert_called_once_with("/entities?page=2", params=None)
