from __future__ import annotations

from bioetl.clients.chembl._base import ChemblEntityClient
from bioetl.core.http.interfaces import ApiTransportProtocol


class ChemblTargetClient(ChemblEntityClient):
    def __init__(self, transport: ApiTransportProtocol) -> None:
        super().__init__(transport, "target")


__all__ = ["ChemblTargetClient"]
