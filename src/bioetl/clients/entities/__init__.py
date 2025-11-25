"""Клиенты ChEMBL entities."""

from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.clients.entities.client_assay import ChemblAssayClient
from bioetl.clients.entities.client_document import ChemblDocumentClient
from bioetl.clients.entities.client_target import ChemblTargetClient
from bioetl.clients.entities.client_testitem import ChemblTestItemClient

__all__ = [
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestItemClient",
]
