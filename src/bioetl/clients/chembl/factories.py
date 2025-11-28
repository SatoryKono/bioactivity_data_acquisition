"""Factories for constructing ChEMBL clients."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from bioetl.clients.chembl.entities import (
    CHEMBL_ALLOWED_ENTITIES,
    ChemblActivityClient,
    ChemblEntity,
    ChemblEntityClientFactory,
    ChemblEntityClientFactoryConfig,
)
from bioetl.clients.chembl.pagination import (
    DEFAULT_PAGINATION_STRATEGY,
    PaginationFactory,
    PaginationStrategy,
    create_pagination_strategy,
)
from bioetl.core.config.models import ChemblAPIConfigModel, PipelineConfig
from bioetl.core.http.config import APIConfig
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.resilience import ResilienceComponents, ResilientRequestExecutorFactory

FACTORIES_DEPRECATION_MESSAGE = (
    "'bioetl.clients.factories' is deprecated; use 'bioetl.clients.chembl.factories'"
)


@dataclass
class _ResilientChemblTransport(ApiTransportProtocol):
    components: ResilienceComponents

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        url = self.components.request_builder.build_url(path)
        merged_headers = self.components.request_builder.merge_headers(headers)
        return self.components.executor.request(
            method,
            url,
            headers=merged_headers,
            params=params,
            json=json,
        )

    def close(self) -> None:
        self.components.request_builder.close()


def _resolve_metadata(config: PipelineConfig | Mapping[str, Any] | None) -> Mapping[str, Any]:
    if config is None:
        return {}
    metadata = getattr(config, "metadata", None)
    if hasattr(metadata, "model_dump"):
        return metadata.model_dump()
    if isinstance(metadata, Mapping):
        return metadata
    return {}


def _build_api_config(config: PipelineConfig | Mapping[str, Any] | None) -> APIConfig:
    metadata = _resolve_metadata(config)
    chembl_api_config = ChemblAPIConfigModel.model_validate(metadata.get("chembl_api") or {})
    return APIConfig(
        base_url=chembl_api_config.base_url,
        timeout_sec=chembl_api_config.timeout_sec,
        max_retries=chembl_api_config.max_retries,
        backoff_factor=chembl_api_config.backoff_factor,
        max_backoff_sec=chembl_api_config.max_backoff_sec,
        rate_limit_calls=chembl_api_config.rate_limit_calls,
        rate_limit_period_sec=chembl_api_config.rate_limit_period_sec,
        cache_enabled=chembl_api_config.cache_enabled,
        cache_ttl_sec=chembl_api_config.cache_ttl_sec,
        circuit_breaker_fail_max=chembl_api_config.circuit_breaker_fail_max,
        circuit_breaker_reset_sec=chembl_api_config.circuit_breaker_reset_sec,
        default_headers=dict(chembl_api_config.default_headers),
        user_agent=chembl_api_config.user_agent,
    )


def _transport_factory_builder(api_config: APIConfig) -> Callable[[], ApiTransportProtocol]:
    def factory() -> ApiTransportProtocol:
        components = ResilientRequestExecutorFactory(api_config).create()
        return _ResilientChemblTransport(components)

    return factory


def default_chembl_factory(
    config: PipelineConfig | Mapping[str, Any] | None,
    *,
    pagination_strategy: PaginationStrategy | None = None,
    pagination_strategy_name: str | None = DEFAULT_PAGINATION_STRATEGY,
    pagination_factories: Mapping[str, PaginationFactory] | None = None,
    transport_factory: Callable[[], ApiTransportProtocol] | None = None,
) -> ChemblEntityClientFactory:
    api_config = _build_api_config(config)
    strategy = pagination_strategy or create_pagination_strategy(
        pagination_strategy_name,
        factories=pagination_factories,
        default=None,
    )
    resolved_transport_factory = transport_factory or _transport_factory_builder(api_config)
    return ChemblEntityClientFactory(
        ChemblEntityClientFactoryConfig(
            resolved_transport_factory,
            pagination_strategy_name=pagination_strategy_name,
            pagination_strategy=strategy,
            pagination_factories=pagination_factories,
        )
    )


def make_chembl_client(
    config: PipelineConfig | Mapping[str, Any] | None,
    entity: ChemblEntity | str = ChemblEntity.ACTIVITY,
    **kwargs: Any,
) -> ChemblActivityClient:
    factory = default_chembl_factory(config, **kwargs)
    entity_name = ChemblEntity(entity).value if entity in CHEMBL_ALLOWED_ENTITIES else str(entity)
    return factory.create(entity_name)


def default_activity_client_factory(
    config: PipelineConfig | Mapping[str, Any] | None,
    **kwargs: Any,
) -> ChemblActivityClient:
    return make_chembl_client(config, ChemblEntity.ACTIVITY, **kwargs)


__all__ = (
    "default_chembl_factory",
    "make_chembl_client",
    "default_activity_client_factory",
    "FACTORIES_DEPRECATION_MESSAGE",
)
