from __future__ import annotations

from bioetl.clients.chembl._base import ChemblEntityClient
from bioetl.core.http.interfaces import ApiTransportProtocol


class ChemblDocumentClient(ChemblEntityClient):
    def __init__(self, transport: ApiTransportProtocol) -> None:
        super().__init__(transport, "document")


__all__ = ["ChemblDocumentClient"]
