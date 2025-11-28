"""Фасад клиентского слоя BioETL.

Пакет предоставляет единый вход в инфраструктурные клиенты и их фабрики:

- общий реестр :data:`FACTORIES`, куда можно регистрировать доменные фабрики
  через :func:`register_factory`, либо пакетно через
  :func:`register_domain_factories` (``chembl``/``enricher`` алиасы);
- хелперы для создания ChEMBL-клиентов из ``bioetl.clients.chembl``.
"""

from __future__ import annotations

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
    default_chembl_factory,
    make_chembl_client,
)

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
