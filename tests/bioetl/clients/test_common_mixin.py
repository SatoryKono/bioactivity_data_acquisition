from __future__ import annotations

from collections.abc import Iterator

import pytest
import structlog
from structlog.testing import capture_logs

from bioetl.clients import client_exceptions
from bioetl.clients.common import ApiClientMixin


class DummyClient(ApiClientMixin):
    def __init__(self) -> None:
        self._logger = structlog.get_logger(__name__).bind(entity="dummy")

    def raise_in_iterator(self) -> Iterator[dict[str, str]]:
        def iterator() -> Iterator[dict[str, str]]:
            raise ValueError("iterator boom")

        return self._wrap_iterator(iterator)


def test_normalize_payload_mapping() -> None:
    client = DummyClient()

    normalized = client._normalize_payload({"a": 1, "b": 2})

    assert normalized == {"a": 1, "b": 2}


def test_normalize_payload_iterable() -> None:
    client = DummyClient()
    payload = [{"a": 1}, {"b": 2}]

    normalized = client._normalize_payload(payload)

    assert normalized == [{"a": 1}, {"b": 2}]


def test_wrap_callable_converts_exceptions_and_preserves_context() -> None:
    client = DummyClient()

    with capture_logs() as logs:
        with pytest.raises(client_exceptions.RequestException):
            client._wrap_callable(lambda: (_ for _ in ()).throw(RuntimeError("call boom")))

    assert any(entry.get("entity") == "dummy" for entry in logs)
    assert any(entry.get("event") == "api_call_failed" for entry in logs)


def test_wrap_iterator_converts_exceptions() -> None:
    client = DummyClient()

    with pytest.raises(client_exceptions.RequestException):
        next(client.raise_in_iterator())
