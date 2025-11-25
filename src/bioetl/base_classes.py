"""Common protocol-based contracts shared across BioETL modules."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

__all__ = ["BaseApiClient"]


@runtime_checkable
class BaseApiClient(Protocol):
    """Protocol describing the minimal HTTP client surface area."""

    def get(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        """Fetch a single resource from ``endpoint`` and return the payload."""

    def batch_get(
        self,
        endpoints: Sequence[str],
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        batch_size: int | None = None,
    ) -> Iterable[Any]:
        """Iterate over a collection of ``endpoints`` yielding payloads for each."""

    def search(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_size: int | None = None,
    ) -> Iterable[Any]:
        """Stream paginated resources for the given ``endpoint``."""

    def close(self) -> None:
        """Release any resources (e.g. sessions) associated with the client."""
