"""BioETL Clients and Integrations."""

from __future__ import annotations

import importlib
import warnings
from types import ModuleType
from typing import Any

from bioetl.clients.exceptions import (
    ConnectionError,  # noqa: A004, pylint: disable=redefined-builtin
    HTTPError,
    RequestException,
    Timeout,
)
from bioetl.clients.chembl import (
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblTargetClient,
    ChemblTestItemClient,
    ChemblEntityClientFactory,
    ChemblEntityClientFactoryConfig,
    ChemblEntityClientFactoryProtocol,
    ChemblEntityClientProtocol,
)
from bioetl.clients.chembl.factories import (
    make_chembl_client,
    default_chembl_factory,
)

_DEPRECATED_HTTP_EXPORTS: dict[str, tuple[str, str]] = {
    "ApiTransportProtocol": ("bioetl.core.http", "ApiTransportProtocol"),
    "EntityClientProtocol": ("bioetl.core.http", "EntityClientProtocol"),
    "PaginationStrategy": ("bioetl.core.http", "PaginationStrategy"),
    "NextLinkPagination": ("bioetl.core.http", "NextLinkPagination"),
    "PageParamPagination": ("bioetl.core.http", "PageParamPagination"),
}

__all__ = [
    "ConnectionError",
    "HTTPError",
    "RequestException",
    "Timeout",
    "ApiTransportProtocol",
    "EntityClientProtocol",
    "PaginationStrategy",
    "NextLinkPagination",
    "PageParamPagination",
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestItemClient",
    "ChemblEntityClientFactory",
    "ChemblEntityClientFactoryConfig",
    "ChemblEntityClientFactoryProtocol",
    "ChemblEntityClientProtocol",
    "make_chembl_client",
    "default_chembl_factory",
]


def __getattr__(name: str) -> Any:
    target = _DEPRECATED_HTTP_EXPORTS.get(name)
    if target:
        module_name, attr_name = target
        warnings.warn(
            (
                "bioetl.clients.%s устарел; импортируйте ``%s`` из "
                "``bioetl.core.http``."
            )
            % (name, attr_name),
            DeprecationWarning,
            stacklevel=2,
        )
        module: ModuleType = importlib.import_module(module_name)
        return getattr(module, attr_name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
