"""Tests for DbBackendProtocol contract."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from bioetl.clients.base import (
    ClientRequest,
    DbBackendProtocol,
    RequestContext,
)
from bioetl.clients.base.paging import Page
from bioetl.clients.base.types import Record


class FakeDbBackend:
    """Simple in-memory implementation used to check protocol shape."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def fetch_one(
        self,
        *,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Record | None:
        self.calls.append({"method": "fetch_one", "route": request.route})
        return {"ok": True}

    def iter_records(
        self,
        *,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Iterator[Record]:
        self.calls.append({"method": "iter_records", "route": request.route})
        yield {"kind": "record"}

    def iter_pages(
        self,
        *,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Iterator[Page]:
        self.calls.append({"method": "iter_pages", "route": request.route})
        yield Page(items=[{"kind": "page"}], has_next=False)

    def metadata(self) -> dict[str, object]:
        return {"backend": "fake-db"}

    def close(self) -> None:  # pragma: no cover - trivial
        self.calls.append({"method": "close"})


def test_dbbackend_protocol_runtime_shape() -> None:
    """Ensure FakeDbBackend can be treated as a DbBackendProtocol."""

    backend: DbBackendProtocol = FakeDbBackend()

    request = ClientRequest(route="test")
    context = RequestContext(source="test-source")

    assert backend.fetch_one(request=request, context=context) == {"ok": True}
    list(backend.iter_records(request=request, context=context))
    list(backend.iter_pages(request=request, context=context))

    meta = backend.metadata()
    assert meta["backend"] == "fake-db"

    backend.close()
    methods = {call["method"] for call in backend.calls}
    assert methods == {"fetch_one", "iter_records", "iter_pages", "close"}
