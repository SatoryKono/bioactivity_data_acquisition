from __future__ import annotations

from collections.abc import Callable
from typing import Any

from bioetl.clients.chembl import (
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblTargetClient,
    ChemblTestItemClient,
)
from bioetl.clients.chembl._base import BaseChemblClient
from bioetl.clients.common import EntityClientProtocol
from bioetl.clients.entities import ChemblEntityClientFactory
from bioetl.config.models import PipelineConfig
from bioetl.core.http import ResilientRequestExecutorFactory, UnifiedAPIClient
from bioetl.core.http.api_client import APIConfig
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import DefaultPaginationStrategy
from bioetl.infra import PaginationRegistry, get_default_pagination_registry


def _resolve_api_config(config: PipelineConfig) -> APIConfig:
    metadata = config.metadata or {}
    chembl_cfg = metadata.get("chembl_api", {}) if isinstance(metadata, dict) else {}

    def _get(key: str, default: Any) -> Any:
        if isinstance(chembl_cfg, dict):
            return chembl_cfg.get(key, default)
        return default

    return APIConfig(
        base_url=str(_get("base_url", "https://www.ebi.ac.uk/chembl/api/data")),
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
        default_headers=dict(_get("default_headers", {})) if isinstance(_get("default_headers", {}), dict) else {},
        user_agent=str(_get("user_agent", "bioetl-chembl-client")),
    )


def default_chembl_factory(
    config: PipelineConfig,
    transport_factory: Callable[[], ApiTransportProtocol] | None = None,
    *,
    pagination_registry: PaginationRegistry | None = None,
    pagination_strategy_name: str | None = None,
) -> dict[str, Callable[[], EntityClientProtocol]]:
    """Построить фабрику клиентов ChEMBL на основе конфигурации."""

    api_config = _resolve_api_config(config)

    def _build_transport() -> ApiTransportProtocol:
        if transport_factory is not None:
            return transport_factory()

        components = ResilientRequestExecutorFactory(api_config).create(
            pagination_strategy=DefaultPaginationStrategy(),
        )
        return BaseChemblClient(
            UnifiedAPIClient(
                api_config,
                request_executor=components.executor,
                request_builder=components.request_builder,
                pagination_strategy=components.pagination_strategy,
            )
        )

    registry = pagination_registry or get_default_pagination_registry()

    entity_factory = ChemblEntityClientFactory(
        _build_transport,
        pagination_registry=registry,
        pagination_strategy_name=pagination_strategy_name,
    )

    return {
        "activity": entity_factory.activity,
        "assay": entity_factory.assay,
        "document": entity_factory.document,
        "target": entity_factory.target,
        "testitem": entity_factory.testitem,
    }


def default_activity_client_factory(
    config: PipelineConfig,
    transport_factory: Callable[[], ApiTransportProtocol] | None = None,
    *,
    pagination_registry: PaginationRegistry | None = None,
    pagination_strategy_name: str | None = None,
) -> ChemblActivityClient:
    """Построить клиент ChEMBL Activity с настройками по умолчанию."""

    factory = default_chembl_factory(
        config,
        transport_factory=transport_factory,
        pagination_registry=pagination_registry,
        pagination_strategy_name=pagination_strategy_name,
    )
    builder = factory.get("activity")
    if builder is None:  # pragma: no cover - защитная проверка
        raise RuntimeError("Activity client builder is not configured")
    client = builder()
    if not isinstance(client, ChemblActivityClient):
        raise TypeError("Configured activity builder did not produce ChemblActivityClient")
    return client


__all__ = ["default_chembl_factory", "default_activity_client_factory"]
