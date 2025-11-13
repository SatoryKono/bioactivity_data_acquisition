"""Document entity clients for ChEMBL API."""

from __future__ import annotations

from bioetl.clients.chembl_config import get_entity_config
from bioetl.clients.client_chembl_entity import ChemblEntityClientBase

__all__ = ["ChemblDocumentTermEntityClient"]


class ChemblDocumentTermEntityClient(ChemblEntityClientBase):
    """Client for retrieving ``document_term`` records from the ChEMBL API."""

    ENTITY_CONFIG = get_entity_config("document_term")

