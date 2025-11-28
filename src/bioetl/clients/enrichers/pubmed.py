from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .base import BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.types import JSONRecordStream


class PubmedClient(BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "pubmed")

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        merged_params = {"title": query, **(params or {})}
        return self._get("/pubmed", params=merged_params)

    def fetch(
        self, pmid: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._get(f"/pubmed/{pmid}", params=params)

    search_by_title = search
    fetch_by_pmid = fetch


__all__ = ["PubmedClient"]
