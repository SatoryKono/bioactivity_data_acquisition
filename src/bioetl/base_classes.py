"""Common protocol-based contracts shared across BioETL modules."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

__all__ = ["BaseApiClient", "EntityClientProtocol", "JSONPayload", "JSONPage"]

JSONPayload = Mapping[str, Any] | list[Mapping[str, Any]]
JSONPage = Iterator[Mapping[str, Any]]


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


@runtime_checkable
class EntityClientProtocol(Protocol):
    """Protocol describing common ChEMBL entity client operations."""

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        """Fetch multiple entities by their identifiers."""

    def fetch_all(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[dict[str, Any]]:
        """Iterate through all available entities using pagination."""

    def close(self) -> None:
        """Release any resources (e.g. sessions) associated with the client."""
