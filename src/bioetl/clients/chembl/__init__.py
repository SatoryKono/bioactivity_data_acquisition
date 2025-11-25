"""Клиенты ChEMBL entities."""

from bioetl.clients.chembl.client_activity import ChemblActivityClient
from bioetl.clients.chembl.client_assay import ChemblAssayClient
from bioetl.clients.chembl.client_document import ChemblDocumentClient
from bioetl.clients.chembl.client_target import ChemblTargetClient
from bioetl.clients.chembl.client_testitem import ChemblTestItemClient

__all__ = [
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestItemClient",
]
