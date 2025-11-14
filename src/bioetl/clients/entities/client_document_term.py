"""Document entity clients for ChEMBL API."""

from __future__ import annotations

from bioetl.clients.chembl_config import get_entity_config
from bioetl.clients.client_chembl_entity_base import ChemblClientProtocol, ChemblEntityFetcherBase

__all__ = ["ChemblDocumentTermEntityClient"]


class ChemblDocumentTermEntityClient(ChemblEntityFetcherBase):
    """Client for retrieving ``document_term`` records from the ChEMBL API."""

    ENTITY_CONFIG = get_entity_config("document_term")

    def __init__(self, chembl_client: ChemblClientProtocol) -> None:
        super().__init__(chembl_client, config=self.ENTITY_CONFIG)

