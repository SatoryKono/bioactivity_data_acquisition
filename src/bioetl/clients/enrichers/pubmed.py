from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from .base import BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient


class PubmedClient(BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "pubmed")

    def search_by_title(self, title: str) -> Iterator[dict[str, Any]]:
        return self._get("/pubmed", params={"title": title})

    def fetch_by_pmid(self, pmid: str) -> Iterator[dict[str, Any]]:
        return self._get(f"/pubmed/{pmid}")


__all__ = ["PubmedClient"]
