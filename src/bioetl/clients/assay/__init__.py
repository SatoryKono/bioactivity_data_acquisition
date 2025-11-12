"""Assay-specific ChEMBL clients."""

from .client_chembl_assay import ChemblAssayClient
from .client_chembl_assay_entity import ChemblAssayEntityClient

__all__ = ["ChemblAssayClient", "ChemblAssayEntityClient"]
