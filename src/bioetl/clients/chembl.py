"""ChEMBL-specific API helpers built on top of :mod:`bioetl.core.api_client`."""

from __future__ import annotations

from typing import Any, Iterable, Iterator, Mapping, Sequence

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

__all__ = ["ChemblClient"]


class ChemblClient:
    """High level client for interacting with the ChEMBL REST API."""

    def __init__(self, client: UnifiedAPIClient) -> None:
        self._client = client
        self._log = UnifiedLogger.get(__name__).bind(component="chembl_client")
        self._status_cache: Mapping[str, Any] | None = None

    # ------------------------------------------------------------------
    # Discovery / handshake
    # ------------------------------------------------------------------

    def handshake(self) -> Mapping[str, Any]:
        """Perform the `/status` handshake once and cache the payload."""

        if self._status_cache is None:
            payload = self._client.get("/status").json()
            self._status_cache = payload
            self._log.info(
                "chembl.handshake",
                chembl_release=payload.get("chembl_db_version"),
                api_version=payload.get("api_version"),
            )
        return self._status_cache

    # ------------------------------------------------------------------
    # Pagination helpers
    # ------------------------------------------------------------------

    def paginate(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 200,
        items_key: str | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Yield records for ``endpoint`` honouring ChEMBL pagination."""

        self.handshake()
        next_url: str | None = endpoint
        query = dict(params or {})
        if page_size:
            query.setdefault("limit", page_size)
        while next_url:
            response = self._client.get(next_url, params=query if next_url == endpoint else None)
            payload: Mapping[str, Any] = response.json()
            for item in self._extract_items(payload, items_key):
                yield item
            next_url = self._next_link(payload)
            query = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_items(
        self,
        payload: Mapping[str, Any],
        items_key: str | None,
    ) -> Iterable[Mapping[str, Any]]:
        if items_key:
            items = payload.get(items_key)
            if isinstance(items, Sequence):
                return [item for item in items if isinstance(item, Mapping)]
            return []
        for key, value in payload.items():
            if key == "page_meta":
                continue
            if isinstance(value, Sequence):
                mappings = [item for item in value if isinstance(item, Mapping)]
                if mappings:
                    return mappings
        return []

    @staticmethod
    def _next_link(payload: Mapping[str, Any]) -> str | None:
        page_meta = payload.get("page_meta")
        if isinstance(page_meta, Mapping):
            next_link = page_meta.get("next")
            if isinstance(next_link, str) and next_link:
                return next_link
        return None
