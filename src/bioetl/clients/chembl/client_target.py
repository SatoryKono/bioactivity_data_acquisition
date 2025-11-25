from __future__ import annotations

from bioetl.clients.chembl._base import BaseChemblClient
from bioetl.core.http.api_client import UnifiedAPIClient


class ChemblTargetClient(BaseChemblClient):
    def __init__(self, api_client: UnifiedAPIClient) -> None:
        super().__init__(api_client, "target")


__all__ = ["ChemblTargetClient"]
