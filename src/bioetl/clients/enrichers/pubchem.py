from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from bioetl.clients.enrichers._base import _BaseEnricherClient
from bioetl.core.http.api_client import UnifiedAPIClient


class PubChemClient(_BaseEnricherClient):
    def __init__(self, api_client: UnifiedAPIClient) -> None:
        super().__init__(api_client, "pubchem")

    def fetch_by_cid(self, cid: str) -> Iterator[dict[str, Any]]:
        return self._get(f"/compound/{cid}")

    def search_by_smiles(self, smiles: str) -> Iterator[dict[str, Any]]:
        return self._get("/compound/search", params={"smiles": smiles})


__all__ = ["PubChemClient"]
