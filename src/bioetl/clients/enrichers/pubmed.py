from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from bioetl.clients.enrichers._base import _BaseEnricherClient
from bioetl.core.http.api_client import UnifiedAPIClient


class PubmedClient(_BaseEnricherClient):
    def __init__(self, api_client: UnifiedAPIClient) -> None:
        super().__init__(api_client, "pubmed")

    def search_by_title(self, title: str) -> list[dict[str, Any]]:
        payload = self._get("/pubmed", params={"title": title})
        if isinstance(payload, Mapping):
            results = payload.get("results")
            if isinstance(results, list):
                return [dict(item) for item in results if isinstance(item, Mapping)]
        return [dict(payload)]

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any]:
        return self._get(f"/pubmed/{pmid}")


__all__ = ["PubmedClient"]
