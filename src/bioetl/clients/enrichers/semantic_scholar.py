from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from .base import BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient


class SemanticScholarClient(BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "semantic_scholar")

    def title_search(self, title: str) -> Iterator[dict[str, Any]]:
        return self._get("/paper/search", params={"query": title})


__all__ = ["SemanticScholarClient"]
