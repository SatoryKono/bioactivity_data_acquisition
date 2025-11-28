"""Сборщики :class:`ChemblDescriptorFactory` для пайплайнов ChEMBL."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from functools import partial
from typing import Any, Callable, Sequence

from bioetl.clients.chembl.factory import (
    ChemblClientFactory,
    ChemblDescriptorFactoryBuilder,
)
from .descriptor_factory import ChemblContextFacade, ChemblDescriptorFactory, FetcherStrategy


def _resolve_chembl_context(config: Mapping[str, Any] | Any) -> Mapping[str, Any]:
    if isinstance(config, Mapping):
        return config.get("sources", {}).get("chembl", {})
    sources = getattr(config, "sources", None)
    if isinstance(sources, Mapping):
        return sources.get("chembl", {})
    return {}


def _build_fetcher_strategies(
    fetcher_strategies: Mapping[str, FetcherStrategy] | None,
    entity_name: str,
    chembl_ctx: Mapping[str, Any],
) -> dict[str, FetcherStrategy]:
    strategies: dict[str, FetcherStrategy] = dict(fetcher_strategies or {})
    fetcher_key = f"{entity_name}_fetcher"
    if fetcher_key not in chembl_ctx:
        return strategies

    fetcher = chembl_ctx[fetcher_key]

    def strategy(
        _context: Mapping[str, Any], _plan: Any, _fetcher=fetcher
    ) -> Callable[[Sequence[str] | None], Any] | None:
        if callable(_fetcher):
            return _fetcher
        if _fetcher is None:
            return None

        def noop(batch: Sequence[str] | None):
            if batch is None:
                return []
            return [{"chembl_id": chembl_id} for chembl_id in batch]

        return noop

    strategies[entity_name] = strategy
    return strategies


def _build_context_facade(
    factory: ChemblClientFactory[ChemblDescriptorFactory],
    chembl_ctx: Mapping[str, Any],
) -> ChemblContextFacade:
    chembl_client = chembl_ctx.get("client")
    pagination_strategy = chembl_ctx.get("pagination_strategy")
    pagination_strategy_name = chembl_ctx.get("pagination_strategy_name")
    pagination_factories = chembl_ctx.get("pagination_factories")
    transport_factory = chembl_ctx.get("transport_factory")
    chembl_release = chembl_ctx.get("chembl_release")

    client_factory = chembl_ctx.get("client_factory")
    if client_factory is None and chembl_client is None:
        client_factory = factory.build_entity_client_factory(
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_factories=pagination_factories,
            transport_factory=transport_factory,
        )
        transport_factory = client_factory.config.transport_factory
        pagination_strategy_name = (
            pagination_strategy_name or client_factory.config.pagination_strategy_name
        )
        pagination_strategy = pagination_strategy or client_factory.config.pagination_strategy
        pagination_factories = pagination_factories or client_factory.config.pagination_factories

    return ChemblContextFacade(
        transport_factory=transport_factory,
        pagination_strategy=pagination_strategy,
        pagination_strategy_name=pagination_strategy_name,
        pagination_factories=pagination_factories,
        chembl_release=chembl_release,
        chembl_client=chembl_client,
        client_factory=client_factory,
    )


def build_descriptor_factory(
    factory: ChemblClientFactory[ChemblDescriptorFactory],
    entity: str,
    *,
    mode: str | None = None,
    fallback_rows: Callable[[Iterable[str], Exception], list[dict[str, Any]]] | None = None,
    sort_fields: Mapping[str, Sequence[str]] | None = None,
    fetcher_strategies: Mapping[str, FetcherStrategy] | None = None,
) -> ChemblDescriptorFactory:
    _ = mode
    chembl_ctx = _resolve_chembl_context(factory.config)
    context_facade = _build_context_facade(factory, chembl_ctx)
    fetchers = _build_fetcher_strategies(fetcher_strategies, entity, chembl_ctx)

    return ChemblDescriptorFactory(
        context_facade,
        fetcher_strategies=fetchers,
        fallback_rows=fallback_rows,
        sort_fields=sort_fields,
    )


def build_pipeline_chembl_factory(
    config: Mapping[str, Any] | Any,
    *,
    fallback_rows: Callable[[Iterable[str], Exception], list[dict[str, Any]]] | None = None,
    sort_fields: Mapping[str, Sequence[str]] | None = None,
    fetcher_strategies: Mapping[str, FetcherStrategy] | None = None,
) -> ChemblClientFactory[ChemblDescriptorFactory]:
    """Собрать ``ChemblClientFactory`` для использования внутри пайплайнов."""

    builder: ChemblDescriptorFactoryBuilder[ChemblDescriptorFactory] = partial(
        build_descriptor_factory,
        fallback_rows=fallback_rows,
        sort_fields=sort_fields,
        fetcher_strategies=fetcher_strategies,
    )
    return ChemblClientFactory(config, builder)


__all__ = [
    "build_descriptor_factory",
    "build_pipeline_chembl_factory",
    "ChemblDescriptorFactoryBuilder",
]
