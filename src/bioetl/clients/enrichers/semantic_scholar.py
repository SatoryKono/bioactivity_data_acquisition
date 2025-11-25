from __future__ import annotations

from typing import Any

from bioetl.clients.enrichers._base import _BaseEnricherClient
from bioetl.core.http.api_client import UnifiedAPIClient


class SemanticScholarClient(_BaseEnricherClient):
    def __init__(self, api_client: UnifiedAPIClient) -> None:
        super().__init__(api_client, "semantic_scholar")

    def title_search(self, title: str) -> dict[str, Any]:
        return self._get("/paper/search", params={"query": title})


__all__ = ["SemanticScholarClient"]
