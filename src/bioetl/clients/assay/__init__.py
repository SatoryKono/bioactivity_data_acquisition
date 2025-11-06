"""Assay-specific ChEMBL clients."""

from .chembl_assay import ChemblAssayClient
from .chembl_assay_entity import ChemblAssayEntityClient

__all__ = ["ChemblAssayClient", "ChemblAssayEntityClient"]

