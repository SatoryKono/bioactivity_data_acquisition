from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from bioetl.base_classes import BaseApiClient
from bioetl.clients.enrichers._base import _BaseEnricherClient


class PubmedClient(_BaseEnricherClient):
    def __init__(self, transport: BaseApiClient) -> None:
        super().__init__(transport, "pubmed")

    def search_by_title(self, title: str) -> Iterator[dict[str, Any]]:
        return self._get("/pubmed", params={"title": title})

    def fetch_by_pmid(self, pmid: str) -> Iterator[dict[str, Any]]:
        return self._get(f"/pubmed/{pmid}")


__all__ = ["PubmedClient"]
