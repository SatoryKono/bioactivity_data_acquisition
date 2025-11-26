from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from bioetl.base_classes import BaseApiClient
from bioetl.clients.enrichers._base import _BaseEnricherClient


class SemanticScholarClient(_BaseEnricherClient):
    def __init__(self, transport: BaseApiClient) -> None:
        super().__init__(transport, "semantic_scholar")

    def title_search(self, title: str) -> Iterator[dict[str, Any]]:
        return self._get("/paper/search", params={"query": title})


__all__ = ["SemanticScholarClient"]
