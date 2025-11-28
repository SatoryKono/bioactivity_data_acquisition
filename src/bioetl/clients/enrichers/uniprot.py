from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from bioetl.clients.enrichers.base import BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.types import JSONRecordStream


class UniProtClient(BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "uniprot")

    def fetch(
        self, uniprot_id: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._get(f"/uniprot/{uniprot_id}", params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        merged_params = {"query": query, **(params or {})}
        return self._get("/uniprot/search", params=merged_params)


__all__ = ["UniProtClient"]
