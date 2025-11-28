from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .base import BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.types import JSONRecordStream


class PubChemClient(BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "pubchem")

    def fetch(
        self, cid: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._get(f"/compound/{cid}", params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        merged_params = {"smiles": query, **(params or {})}
        return self._get("/compound/search", params=merged_params)

    fetch_by_cid = fetch
    search_by_smiles = search


__all__ = ["PubChemClient"]
