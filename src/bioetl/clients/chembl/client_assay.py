from __future__ import annotations

from bioetl.base_classes import BaseApiClient
from bioetl.clients.chembl._base import BaseChemblClient


class ChemblAssayClient(BaseChemblClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "assay")


__all__ = ["ChemblAssayClient"]
