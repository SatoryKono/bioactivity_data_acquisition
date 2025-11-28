"""Tests for legacy compatibility helpers in ChEMBL clients."""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from bioetl.clients.chembl.base import BaseChemblClient
from bioetl.clients.chembl.compat import ChemblCompatibilityMixin


class _DummyCompatClient(ChemblCompatibilityMixin):
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self._payload = {"payload": "ok"}

    def fetch_many(
        self,
        *,
        page_size: int = 1000,
        params: dict[str, Any] | None = None,
        page_key: str = "page",
        next_key: str = "next",
        page_param: str | None = "page_param",
    ) -> Iterator[dict[str, Any]]:
        self.calls.append(
            {
                "page_size": page_size,
                "params": params,
                "page_key": page_key,
                "next_key": next_key,
                "page_param": page_param,
            }
        )
        return iter([self._payload])


def test_base_client_uses_compat_mixin() -> None:
    """BaseChemblClient should inherit deprecated compatibility helpers."""

    assert ChemblCompatibilityMixin in BaseChemblClient.__mro__


def test_fetch_page_warns_and_delegates() -> None:
    client = _DummyCompatClient()

    with pytest.warns(DeprecationWarning, match="fetch_page"):
        result = list(
            client.fetch_page(
                page_size=10,
                params={"foo": "bar"},
                page_key="custom_page",
                next_key="custom_next",
                page_param="page_param",
            )
        )

    assert result == [client._payload]
    assert client.calls == [
        {
            "page_size": 10,
            "params": {"foo": "bar"},
            "page_key": "custom_page",
            "next_key": "custom_next",
            "page_param": "page_param",
        }
    ]


def test_list_warns_and_delegates() -> None:
    client = _DummyCompatClient()

    with pytest.warns(DeprecationWarning, match="list is deprecated"):
        list(client.list(page_size=5))

    assert client.calls == [
        {
            "page_size": 5,
            "params": None,
            "page_key": "page",
            "next_key": "next",
            "page_param": "page_param",
        }
    ]


def test_fetch_all_warns_and_delegates() -> None:
    client = _DummyCompatClient()

    with pytest.warns(DeprecationWarning, match="fetch_all"):
        list(client.fetch_all(params={"limit": 3}))

    assert client.calls == [
        {
            "page_size": 1000,
            "params": {"limit": 3},
            "page_key": "page",
            "next_key": "next",
            "page_param": "page_param",
        }
    ]
