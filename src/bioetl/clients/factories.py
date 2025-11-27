"""Factory functions for creating ChEMBL clients."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from bioetl.clients.chembl.adapter import ChemblTransportAdapter
from bioetl.clients.chembl.entities import (
    CHEMBL_ALLOWED_ENTITIES,
    ChemblActivityClient,
    ChemblEntityClientFactory,
)
from bioetl.clients.pagination import (
    PaginationFactory,
    create_pagination_strategy,
)
from bioetl.core.config.models import PipelineConfig
from bioetl.core.http import (
    ResilientRequestExecutorFactory,
    UnifiedAPIClient,
)
from bioetl.core.http.api_client import APIConfig
from bioetl.core.http.api_entity_client import EntityClientProtocol
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import (
    DefaultPaginationStrategy,
    PaginationStrategy,
)


def _resolve_api_config(config: PipelineConfig) -> APIConfig:
    if isinstance(config, dict):
        metadata = config.get("metadata") or {}
    else:
        metadata = getattr(config, "metadata", {}) or {}

    chembl_cfg = {}
    if isinstance(metadata, dict):
        chembl_cfg = metadata.get("chembl_api", {})

    def _get(key: str, default: Any) -> Any:
        if isinstance(chembl_cfg, dict):
            return chembl_cfg.get(key, default)
        return default

    default_headers = _get("default_headers", {})
    if not isinstance(default_headers, dict):
        default_headers = {}

    return APIConfig(
        base_url=str(
            _get("base_url", "https://www.ebi.ac.uk/chembl/api/data")
        ),
        timeout_sec=float(_get("timeout_sec", 30)),
        max_retries=int(_get("max_retries", 3)),
        backoff_factor=float(_get("backoff_factor", 1)),
        max_backoff_sec=float(_get("max_backoff_sec", 30)),
        rate_limit_calls=int(_get("rate_limit_calls", 10)),
        rate_limit_period_sec=float(_get("rate_limit_period_sec", 1.0)),
        cache_enabled=bool(_get("cache_enabled", True)),
        cache_ttl_sec=int(_get("cache_ttl_sec", 300)),
        circuit_breaker_fail_max=int(_get("circuit_breaker_fail_max", 5)),
        circuit_breaker_reset_sec=int(_get("circuit_breaker_reset_sec", 60)),
        default_headers=dict(default_headers),
        user_agent=str(_get("user_agent", "bioetl-chembl-client")),
    )


def default_chembl_factory(
    config: PipelineConfig,
    transport_factory: Callable[[], ApiTransportProtocol] | None = None,
    *,
    pagination_strategy_name: str | None = None,
    pagination_factories: Mapping[str, PaginationFactory] | None = None,
) -> dict[str, Callable[[], EntityClientProtocol] | ChemblEntityClientFactory]:
    """Построить фабрику клиентов ChEMBL на основе конфигурации."""

    api_config = _resolve_api_config(config)
    pagination_strategy: PaginationStrategy | None = None
    if pagination_strategy_name:
        pagination_strategy = create_pagination_strategy(
            pagination_strategy_name,
            factories=pagination_factories,
        )

    def _build_transport() -> ApiTransportProtocol:
        if transport_factory is not None:
            return transport_factory()

        components = ResilientRequestExecutorFactory(api_config).create(
            pagination_strategy=DefaultPaginationStrategy(),
        )
        return ChemblTransportAdapter(
            UnifiedAPIClient(
                api_config,
                request_executor=components.executor,
                request_builder=components.request_builder,
                pagination_strategy=components.pagination_strategy,
            )
        )

    entity_factory = ChemblEntityClientFactory(
        _build_transport,
        pagination_strategy=pagination_strategy,
        pagination_strategy_name=pagination_strategy_name,
        pagination_factories=pagination_factories,
    )

    return {
        "chembl": entity_factory,
        "activity": entity_factory.activity,
        "assay": entity_factory.assay,
        "document": entity_factory.document,
        "target": entity_factory.target,
        "testitem": entity_factory.testitem,
    }


def make_chembl_client(
    entity: str,
    transport: ApiTransportProtocol,
    *,
    pagination_strategy: PaginationStrategy | None = None,
    pagination_strategy_name: str | None = None,
    pagination_factories: Mapping[str, PaginationFactory] | None = None,
):
    """Построить клиента ChEMBL для разрешённой сущности.

    Поддерживает явную валидацию сущностей через ``CHEMBL_ALLOWED_ENTITIES``.
    """

    if entity not in CHEMBL_ALLOWED_ENTITIES:
        msg = f"Unsupported ChEMBL entity: {entity}"
        raise ValueError(msg)

    factory = ChemblEntityClientFactory(
        lambda: transport,
        pagination_strategy=pagination_strategy,
        pagination_strategy_name=pagination_strategy_name,
        pagination_factories=pagination_factories,
    )
    return factory.create(entity)


def default_activity_client_factory(
    config: PipelineConfig,
    transport_factory: Callable[[], ApiTransportProtocol] | None = None,
    *,
    pagination_strategy_name: str | None = None,
    pagination_factories: Mapping[str, PaginationFactory] | None = None,
) -> ChemblActivityClient:
    """Построить клиент ChEMBL Activity с настройками по умолчанию."""

    factory = default_chembl_factory(
        config,
        transport_factory=transport_factory,
        pagination_strategy_name=pagination_strategy_name,
        pagination_factories=pagination_factories,
    )
    builder = factory.get("activity")
    if builder is None:  # pragma: no cover - защитная проверка
        raise RuntimeError("Activity client builder is not configured")
    client = builder()
    if not isinstance(client, ChemblActivityClient):
        print(f"DEBUG: client type: {type(client)}")
        print(f"DEBUG: expected type: {ChemblActivityClient}")
        print(f"DEBUG: client module: {client.__class__.__module__}")
        print(f"DEBUG: expected module: {ChemblActivityClient.__module__}")
        raise TypeError(
            "Configured activity builder did not produce ChemblActivityClient"
        )
    return client


__all__ = [
    "default_chembl_factory",
    "default_activity_client_factory",
    "make_chembl_client",
]
