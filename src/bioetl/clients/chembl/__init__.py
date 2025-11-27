"""
ChEMBL API clients and related utilities.

This package provides clients for interacting with the ChEMBL API, including
entity-specific clients, transport adapters, and normalization logic.
"""
from bioetl.clients.chembl.base import (
    BaseChemblClient,
    ChemblEntityClient,
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
)
from bioetl.clients.chembl.adapter import ChemblTransportAdapter
from bioetl.core.pipeline.unified import ChemblExtractionServiceDescriptor
from bioetl.clients.chembl.entities import (
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblTargetClient,
    ChemblTestItemClient,
    ChemblEntityClientFactory,
    CHEMBL_ALLOWED_ENTITIES,
)
from bioetl.clients.chembl.normalization import (
    BaseChemblNormalizer,
    ColumnMapping,
    ColumnNormalizationSpec,
    build_records_from_payload,
)

__all__ = [
    "BaseChemblClient",
    "ChemblTransportAdapter",
    "ChemblEntityClient",
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_PAGE_PARAM",
    "ChemblExtractionServiceDescriptor",
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestItemClient",
    "ChemblEntityClientFactory",
    "CHEMBL_ALLOWED_ENTITIES",
    "BaseChemblNormalizer",
    "ColumnMapping",
    "ColumnNormalizationSpec",
    "build_records_from_payload",
]
