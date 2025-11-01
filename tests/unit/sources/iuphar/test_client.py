"""Unit tests for the IUPHAR API client wrapper."""

from __future__ import annotations

from unittest.mock import create_autospec

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.sources.iuphar.client import IupharClient


def test_iuphar_client_proxies_json_requests() -> None:
    """The client should pass through path and params to the underlying API."""

    api = create_autospec(UnifiedAPIClient, instance=True)
    client = IupharClient(api=api)

    params = {"page": 1, "page_size": 200}
    client.request_json("/targets", params=params)

    api.request_json.assert_called_once_with("/targets", params=params)


def test_iuphar_client_closes_underlying_session() -> None:
    """The close helper should delegate to the wrapped API client."""

    api = create_autospec(UnifiedAPIClient, instance=True)
    client = IupharClient(api=api)

    client.close()

    api.close.assert_called_once()
