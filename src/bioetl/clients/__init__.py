"""BioETL Clients and Integrations."""

from __future__ import annotations

import importlib
import warnings
from types import ModuleType
from typing import Any

from bioetl.clients.base import (
    FACTORIES,
    ClientFactory,
    ClientProtocol,
    get_factory,
    register_domain_factories,
    register_factory,
)
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

_DEPRECATED_PAGINATION_HELPERS: dict[str, tuple[str, str]] = {
    "DEFAULT_NEXT_KEY": ("bioetl.core.http.pagination_helpers", "DEFAULT_NEXT_KEY"),
    "DEFAULT_PAGE_KEY": ("bioetl.core.http.pagination_helpers", "DEFAULT_PAGE_KEY"),
    "DEFAULT_PAGE_PARAM": ("bioetl.core.http.pagination_helpers", "DEFAULT_PAGE_PARAM"),
    "fetch_all_entities": ("bioetl.core.http.pagination_helpers", "fetch_all_entities"),
    "iter_ids": ("bioetl.core.http.pagination_helpers", "iter_ids"),
    "iter_pages": ("bioetl.core.http.pagination_helpers", "iter_pages"),
    "iterate_entity_records": (
        "bioetl.core.http.pagination_helpers",
        "iterate_entity_records",
    ),
    "iterate_records": ("bioetl.core.http.pagination_helpers", "iterate_records"),
    "list_entities": ("bioetl.core.http.pagination_helpers", "list_entities"),
    "normalize_payload": (
        "bioetl.core.http.pagination_helpers",
        "normalize_payload",
    ),
    "warn_fetch_all": ("bioetl.core.http.pagination_helpers", "warn_fetch_all"),
}

__all__ = [
    "ConnectionError",
    "HTTPError",
    "RequestException",
    "Timeout",
    "ClientProtocol",
    "ClientFactory",
    "FACTORIES",
    "register_factory",
    "register_domain_factories",
    "get_factory",
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
    *_DEPRECATED_PAGINATION_HELPERS.keys(),
]


def __getattr__(name: str) -> Any:
    target = _DEPRECATED_HTTP_EXPORTS.get(name) or _DEPRECATED_PAGINATION_HELPERS.get(name)
    if target:
        module_name, attr_name = target
        warnings.warn(
            (
                "bioetl.clients.%s устарел; импортируйте ``%s`` из "
                "``%s``."
            )
            % (name, attr_name, module_name),
            DeprecationWarning,
            stacklevel=2,
        )
        module: ModuleType = importlib.import_module(module_name)
        return getattr(module, attr_name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
