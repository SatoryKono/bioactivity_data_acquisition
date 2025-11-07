"""Document-specific ChEMBL clients."""

from .chembl_document import ChemblDocumentClient
from .chembl_document_entity import ChemblDocumentTermEntityClient

__all__ = ["ChemblDocumentClient", "ChemblDocumentTermEntityClient"]
