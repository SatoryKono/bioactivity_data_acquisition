from __future__ import annotations

import pytest
from structlog.testing import capture_logs

from bioetl.clients import client_exceptions
from bioetl.clients.common import ApiClientMixin


class _DummyMixinConsumer(ApiClientMixin):
    def __init__(self) -> None:
        self.entity = "dummy"
        self.api_client = None
        self._logger = __import__("structlog").get_logger(__name__).bind(entity=self.entity)


def test_normalize_payload_supports_mapping_and_iterable():
    client = _DummyMixinConsumer()

    mapping_payload = {"results": [{"id": 1}, {"id": 2}], "next": None}
    iterable_payload = ({"id": 3}, {"id": 4})

    assert list(client._normalize_payload(mapping_payload)) == [
        {"id": 1},
        {"id": 2},
    ]
    assert list(client._normalize_payload(iterable_payload)) == [
        {"id": 3},
        {"id": 4},
    ]


def test_wrap_callable_converts_exceptions_to_request_exception():
    client = _DummyMixinConsumer()

    with pytest.raises(client_exceptions.RequestException):
        client._wrap_callable(lambda: (_ for _ in ()).throw(ValueError("boom")))


def test_wrap_callable_preserves_log_context():
    client = _DummyMixinConsumer()

    with capture_logs() as logs, pytest.raises(client_exceptions.RequestException):
        client._wrap_callable(
            lambda: (_ for _ in ()).throw(ValueError("boom")),
            log_context={"path": "/dummy"},
        )

    assert logs
    assert logs[0]["entity"] == "dummy"
    assert logs[0]["path"] == "/dummy"
    assert logs[0]["event"] == "api_call_failed"
