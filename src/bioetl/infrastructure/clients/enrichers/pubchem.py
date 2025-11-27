from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from ._base import _BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient


class PubChemClient(_BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "pubchem")

    def fetch_by_cid(self, cid: str) -> Iterator[dict[str, Any]]:
        return self._get(f"/compound/{cid}")

    def search_by_smiles(self, smiles: str) -> Iterator[dict[str, Any]]:
        return self._get("/compound/search", params={"smiles": smiles})


__all__ = ["PubChemClient"]
