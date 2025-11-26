from __future__ import annotations

from bioetl.base_classes import BaseApiClient
from bioetl.clients.chembl._base import ChemblEntityClient


class ChemblTargetClient(ChemblEntityClient):
    def __init__(self, transport: BaseApiClient) -> None:
        super().__init__(transport, "target")


__all__ = ["ChemblTargetClient"]
