from __future__ import annotations

from bioetl.clients.chembl._base import BaseChemblClient
from bioetl.core.http.api_client import UnifiedAPIClient


class ChemblAssayClient(BaseChemblClient):
    def __init__(self, api_client: UnifiedAPIClient) -> None:
        super().__init__(api_client, "assay")


__all__ = ["ChemblAssayClient"]
