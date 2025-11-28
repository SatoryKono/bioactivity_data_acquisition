from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .base import BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.types import JSONRecordStream


class SemanticScholarClient(BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "semantic_scholar")

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        merged_params = {"query": query, **(params or {})}
        return self._get("/paper/search", params=merged_params)

    def fetch(
        self, paper_id: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._get(f"/paper/{paper_id}", params=params)

    title_search = search


__all__ = ["SemanticScholarClient"]
