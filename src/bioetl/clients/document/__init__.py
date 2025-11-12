"""Document-specific ChEMBL clients."""

from .client_chembl_document import ChemblDocumentClient
from .client_chembl_document_entity import ChemblDocumentTermEntityClient

__all__ = ["ChemblDocumentClient", "ChemblDocumentTermEntityClient"]
