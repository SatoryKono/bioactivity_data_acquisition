"""Common protocol-based contracts shared across BioETL modules."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "BaseApiClient",
    "JSONPayload",
    "JSONPage",
    "JSONRecord",
    "JSONRecordStream",
]

JSONPayload = Mapping[str, Any] | list[Mapping[str, Any]]
JSONPage = Iterator[Mapping[str, Any]]
JSONRecord = Mapping[str, Any]
JSONRecordStream = Iterator[JSONRecord]


@runtime_checkable
class BaseApiClient(Protocol):
    """Protocol describing the minimal HTTP client surface area.

    Implementations must raise ``bioetl.clients.client_exceptions`` errors
    (``HTTPError``, ``Timeout``, ``ConnectionError``, ``RequestException``)
    for network-related failures. Successful calls are expected to return
    either a mapping or a list of mappings to make downstream consumption
    predictable.
    """

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> JSONPayload:
        """Fetch a single resource from ``endpoint`` and return decoded JSON."""

    def paginate_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> JSONPage:
        """Iterate over paginated JSON resources for the given ``endpoint``."""

    def close(self) -> None:
        """Release any resources (e.g. sessions) associated with the client."""


# ---------------------------------------------------------------------------
# High-level client expectations
# ---------------------------------------------------------------------------
#
# All domain-specific HTTP clients (ChEMBL, enrichment providers, etc.) are
# expected to expose record-level iterators to upstream pipeline code. The
# canonical format is ``Iterator[Mapping[str, Any]]`` (``JSONRecordStream``),
# produced by flattening any ``results`` collections in API responses and
# normalising individual mappings. This keeps extraction code streaming-friendly
# and avoids special cases for payload shapes across providers.
