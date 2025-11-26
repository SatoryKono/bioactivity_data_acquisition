from __future__ import annotations

from unittest.mock import MagicMock

from bioetl.clients.common import NextLinkPagination, PageParamPagination


def test_next_link_pagination_traverses_next_links_and_params() -> None:
    transport = MagicMock()
    transport.request.return_value = {"results": [{"id": 3}], "next": None}

    initial_page = {"results": [{"id": 1}, {"id": 2}], "next": "/entities?page=2"}

    logger = MagicMock()
    strategy = NextLinkPagination()

    pages = list(
        strategy.iter_pages(
            initial_page,
            transport,
            endpoint="/entities",
            params={"limit": 2},
            logger=logger,
        )
    )

    assert pages == [initial_page, {"results": [{"id": 3}], "next": None}]
    transport.request.assert_called_once_with("GET", "/entities?page=2", params=None)
    logger.info.assert_called_once_with("api_call", path="/entities?page=2")


def test_next_link_pagination_yields_payload_without_results() -> None:
    transport = MagicMock()
    strategy = NextLinkPagination()

    initial_page = {"value": 42}

    pages = list(strategy.iter_pages(initial_page, transport, endpoint="/entities"))

    assert pages == [initial_page]
    transport.request.assert_not_called()


def test_page_param_pagination_uses_page_param_and_flattens_results() -> None:
    transport = MagicMock()
    transport.request.side_effect = [
        {"results": [{"id": 2}, {"id": 3}], "next": None},
    ]

    strategy = PageParamPagination(page_param="page")
    initial_page = {"results": [{"id": 1}]}

    pages = list(
        strategy.iter_pages(
            initial_page,
            transport,
            endpoint="/entities",
            params={"limit": 50, "foo": "bar"},
        )
    )

    assert pages == [initial_page, {"results": [{"id": 2}, {"id": 3}], "next": None}]
    transport.request.assert_called_once_with(
        "GET", "/entities", params={"limit": 50, "foo": "bar", "page": 2}
    )


def test_page_param_pagination_falls_back_without_results() -> None:
    transport = MagicMock()
    strategy = PageParamPagination()

    initial_page = {"value": "fallback"}

    pages = list(strategy.iter_pages(initial_page, transport, endpoint="/entities"))

    assert pages == [initial_page]
    transport.request.assert_not_called()
