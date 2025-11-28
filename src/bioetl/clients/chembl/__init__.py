"""
ChEMBL API clients and related utilities.

This package provides clients for interacting with the ChEMBL API, including
entity-specific clients, transport adapters, and normalization logic.
"""
from typing import TYPE_CHECKING

from bioetl.clients.chembl.base import (
    BaseChemblClient,
    ChemblEntityClient,
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
)
from bioetl.clients.chembl.adapter import ChemblTransportAdapter
from bioetl.clients.chembl.entities import (
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblTargetClient,
    ChemblTestItemClient,
    ChemblEntityClientFactory,
    CHEMBL_ALLOWED_ENTITIES,
)
from bioetl.clients.chembl.factories import (
    default_activity_client_factory,
    default_chembl_factory,
    make_chembl_client,
)
from bioetl.clients.chembl.normalization import (
    BaseChemblNormalizer,
    ColumnMapping,
    ColumnNormalizationSpec,
    build_records_from_payload,
)
from bioetl.clients.chembl.pagination import (
    DEFAULT_PAGINATION_STRATEGY,
    PaginationFactory,
    PaginationStrategy,
    available_pagination_strategies,
    create_pagination_strategy,
    register_pagination_strategy,
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
    "default_activity_client_factory",
    "default_chembl_factory",
    "make_chembl_client",
    "PaginationStrategy",
    "PaginationFactory",
    "DEFAULT_PAGINATION_STRATEGY",
    "available_pagination_strategies",
    "create_pagination_strategy",
    "register_pagination_strategy",
]

if TYPE_CHECKING:
    from bioetl.core.pipeline.unified import ChemblExtractionServiceDescriptor


def __getattr__(name: str):
    if name == "ChemblExtractionServiceDescriptor":
        from bioetl.core.pipeline.unified import ChemblExtractionServiceDescriptor

        return ChemblExtractionServiceDescriptor
    msg = f"module '{__name__}' has no attribute '{name}'"
    raise AttributeError(msg)
