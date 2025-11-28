"""Public ChEMBL client surface and helpers.

The package root exposes only the supported client classes and factories.
Other internal helpers can be imported directly from their submodules; legacy
aliases now emit :class:`DeprecationWarning` when accessed.
"""

from __future__ import annotations

import importlib
import warnings
from typing import TYPE_CHECKING, Any

from bioetl.clients.chembl.base import BaseChemblClient, ChemblEntityClient
from bioetl.clients.chembl.entities import (
    CHEMBL_ALLOWED_ENTITIES,
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblEntityClientFactory,
    ChemblEntityClientFactoryConfig,
    ChemblEntityClientFactoryProtocol,
    ChemblEntityClientProtocol,
    ChemblTargetClient,
    ChemblTestItemClient,
)
from bioetl.clients.chembl.factories import (
    default_activity_client_factory,
    default_chembl_factory,
    make_chembl_client,
)
from bioetl.clients.chembl.factory import ChemblClientFactory
from bioetl.clients.chembl.normalization import (
    BaseChemblNormalizer,
    ColumnMapping,
    ColumnNormalizationSpec,
    build_records_from_payload,
)

if TYPE_CHECKING:  # pragma: no cover - import-time only
    from bioetl.clients.chembl.adapter import ChemblTransportAdapter
    from bioetl.clients.chembl.base import BaseChemblEntityProtocol, ChemblClientProtocol
    from bioetl.clients.chembl.pagination import (
        DEFAULT_PAGINATION_STRATEGY,
        PaginationFactory,
        PaginationStrategy,
        available_pagination_strategies,
        create_pagination_strategy,
        register_pagination_strategy,
    )
    from bioetl.core.pipeline.unified import ChemblExtractionServiceDescriptor

__all__ = [
    "BaseChemblClient",
    "ChemblEntityClient",
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestItemClient",
    "ChemblEntityClientFactory",
    "ChemblEntityClientFactoryConfig",
    "ChemblEntityClientFactoryProtocol",
    "ChemblEntityClientProtocol",
    "CHEMBL_ALLOWED_ENTITIES",
    "BaseChemblNormalizer",
    "ColumnMapping",
    "ColumnNormalizationSpec",
    "build_records_from_payload",
    "default_activity_client_factory",
    "default_chembl_factory",
    "make_chembl_client",
    "ChemblClientFactory",
]

_DEPRECATED_EXPORTS: dict[str, tuple[str, str]] = {
    "BaseChemblEntityProtocol": (
        "bioetl.clients.chembl.base",
        "BaseChemblEntityProtocol",
    ),
    "ChemblClientProtocol": (
        "bioetl.clients.chembl.base",
        "ChemblClientProtocol",
    ),
    "DEFAULT_NEXT_KEY": ("bioetl.core.http.pagination", "DEFAULT_NEXT_KEY"),
    "DEFAULT_PAGE_KEY": ("bioetl.core.http.pagination", "DEFAULT_PAGE_KEY"),
    "DEFAULT_PAGE_PARAM": ("bioetl.core.http.pagination", "DEFAULT_PAGE_PARAM"),
    "ChemblTransportAdapter": (
        "bioetl.clients.chembl.adapter",
        "ChemblTransportAdapter",
    ),
    "ChemblExtractionServiceDescriptor": (
        "bioetl.core.pipeline.unified",
        "ChemblExtractionServiceDescriptor",
    ),
    "PaginationStrategy": (
        "bioetl.clients.chembl.pagination",
        "PaginationStrategy",
    ),
    "PaginationFactory": (
        "bioetl.clients.chembl.pagination",
        "PaginationFactory",
    ),
    "DEFAULT_PAGINATION_STRATEGY": (
        "bioetl.clients.chembl.pagination",
        "DEFAULT_PAGINATION_STRATEGY",
    ),
    "available_pagination_strategies": (
        "bioetl.clients.chembl.pagination",
        "available_pagination_strategies",
    ),
    "create_pagination_strategy": (
        "bioetl.clients.chembl.pagination",
        "create_pagination_strategy",
    ),
    "register_pagination_strategy": (
        "bioetl.clients.chembl.pagination",
        "register_pagination_strategy",
    ),
}


def __getattr__(name: str) -> Any:
    deprecated = _DEPRECATED_EXPORTS.get(name)
    if deprecated:
        module_name, attr_name = deprecated
        warnings.warn(
            (
                "`bioetl.clients.chembl.%s` устарел; импортируйте ``%s`` "
                "из ``%s``."
            )
            % (name, attr_name, module_name),
            DeprecationWarning,
            stacklevel=2,
        )
        return getattr(importlib.import_module(module_name), attr_name)
    msg = f"module '{__name__}' has no attribute '{name}'"
    raise AttributeError(msg)
