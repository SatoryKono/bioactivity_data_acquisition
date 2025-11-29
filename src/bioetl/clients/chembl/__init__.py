"""Публичная поверхность ChEMBL-клиентов и фабрик.

Точка входа включает готовые фабрики адаптеров, нормализаторы и reexport’ы
для удобной сборки клиентов ChEMBL через :func:`make_chembl_client` или
:func:`default_chembl_factory`. Внутренние хелперы доступны из подмодулей,
а устаревшие алиасы при обращении выдают :class:`DeprecationWarning` и
проксируют к новым API (пагинация, протоколы транспорта и т.д.).
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
    BaseChemblAdapterFactory,
    TransportFactoryRegistry,
    default_activity_client_factory,
    default_chembl_factory,
    make_chembl_client,
)
from bioetl.clients.chembl.factory import (
    ChemblClientFactory,
    ChemblDescriptorFactoryBuilder,
)
from bioetl.clients.chembl.normalization import (
    BaseChemblNormalizer,
    ColumnMapping,
    ColumnNormalizationSpec,
    build_records_from_payload,
)
from bioetl.clients.chembl.strategy_resolver import (
    PaginationStrategyResolverMixin,
)
from bioetl.clients.chembl.client import ChemblClient, ChemblResourceConfig
from bioetl.clients.chembl.compat import ChemblCompatibilityMixin

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

__all__ = [
    "BaseChemblClient",
    "ChemblEntityClient",
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestItemClient",
    "BaseChemblAdapterFactory",
    "TransportFactoryRegistry",
    "PaginationStrategyResolverMixin",
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
    "ChemblDescriptorFactoryBuilder",
    "ChemblClient",
    "ChemblResourceConfig",
    "ChemblCompatibilityMixin",
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
    "ChemblTransportAdapter": (
        "bioetl.clients.chembl.adapter",
        "ChemblTransportAdapter",
    ),
    "ChemblExtractionServiceDescriptor": (
        "bioetl.core.pipeline.unified",
        "ChemblExtractionServiceDescriptor",
    ),
}


def __getattr__(name: str) -> Any:
    deprecated = _DEPRECATED_EXPORTS.get(name)
    if deprecated:
        module_name, attr_name = deprecated
        warnings.warn(
            f"`bioetl.clients.chembl.{name}` устарел; импортируйте "
            f"``{attr_name}``` из ``{module_name}``.",
            DeprecationWarning,
            stacklevel=2,
        )
        return getattr(importlib.import_module(module_name), attr_name)
    msg = f"module '{__name__}' has no attribute '{name}'"
    raise AttributeError(msg)
