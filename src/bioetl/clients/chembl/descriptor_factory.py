"""Factories for building ChemblExtractionServiceDescriptor instances."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence, cast

from bioetl.clients.chembl.entities import (
    ChemblEntityClientFactory,
    ChemblEntityClientFactoryConfig,
)
from bioetl.clients.chembl.pagination import (
    PaginationFactory,
)
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy
from bioetl.core.pipeline.unified import ChemblExtractionServiceDescriptor


@dataclass(slots=True)
class BatchPlan:
    """Batch parameters used during extraction."""

    batch_size: int | None = None
    chunk_size: int | None = None


FetcherStrategy = Callable[
    [Mapping[str, Any], BatchPlan | None],
    Callable[[Sequence[str] | None], Any] | None,
]


@dataclass
class ChemblContextFacade:
    """Unified access point for ChEMBL HTTP context and metadata."""

    transport_factory: Callable[[], ApiTransportProtocol] | None = None
    pagination_strategy: PaginationStrategy | None = None
    pagination_strategy_name: str | None = None
    pagination_factories: Mapping[str, PaginationFactory] | None = None
    chembl_release: str | None = None
    chembl_client: Any | None = None
    client_factory: ChemblEntityClientFactory | None = None
    _built_factory: ChemblEntityClientFactory | None = field(
        default=None, init=False
    )

    def _ensure_factory(self) -> ChemblEntityClientFactory:
        if self.client_factory is not None:
            return self.client_factory
        if self._built_factory is None:
            if self.transport_factory is None:
                msg = (
                    "transport_factory is required "
                    "when client_factory is not provided"
                )
                raise RuntimeError(msg)
            config = ChemblEntityClientFactoryConfig(
                self.transport_factory,
                pagination_strategy_name=self.pagination_strategy_name,
                pagination_strategy=self.pagination_strategy,
                pagination_factories=self.pagination_factories,
            )
            self._built_factory = ChemblEntityClientFactory(config)
        return self._built_factory

    def create_client(self, entity_name: str) -> Any:
        """Build or return a ChEMBL entity client for the given entity."""

        if self.chembl_client is not None:
            return self.chembl_client
        factory = self._ensure_factory()
        return factory.create(entity_name)


class ChemblDescriptorFactory:
    """Builds service descriptors without direct access to pipeline config."""

    def __init__(
        self,
        context: ChemblContextFacade,
        *,
        fetcher_strategies: Mapping[str, FetcherStrategy] | None = None,
        fallback_rows: (
            Callable[[Iterable[str], Exception], list[dict[str, Any]]] | None
        ) = None,
        sort_fields: Mapping[str, Sequence[str]] | None = None,
    ) -> None:
        self._context = context
        self._fetcher_strategies = dict(fetcher_strategies or {})
        self._fallback_rows = fallback_rows
        self._sort_fields = {
            k: tuple(v) for k, v in (sort_fields or {}).items()
        }

    def _wrap_fetcher(
        self, fetcher: Callable[[Sequence[str] | None], Any]
    ) -> Callable[[Sequence[str] | None], Any]:
        if self._fallback_rows is None:
            return fetcher

        def wrapped(batch: Sequence[str] | None) -> Any:
            try:
                return fetcher(batch)
            except Exception as exc:  # pragma: no cover - defensive guard
                if batch:
                    # MyPy: self._fallback_rows is not None due to outer check
                    fallback_fn = cast(
                        Callable[
                            [Iterable[str], Exception],
                            list[dict[str, Any]],
                        ],
                        self._fallback_rows,
                    )
                    rows = fallback_fn(batch, exc)
                    return rows, {"fallback": len(rows)}
                raise

        return wrapped

    def build(
        self,
        entity_name: str,
        *,
        mode: str = "chembl",
        batch_plan: BatchPlan | None = None,
    ) -> ChemblExtractionServiceDescriptor[Any]:
        """Assemble a descriptor for the requested entity."""

        def build_context(_pipeline: Any) -> Mapping[str, Any]:
            chembl_client = self._context.create_client(entity_name)
            context: dict[str, Any] = {
                "chembl_client": chembl_client,
                "mode": mode,
            }
            if batch_plan and batch_plan.chunk_size:
                context["page_size"] = batch_plan.chunk_size
            if self._context.chembl_release:
                context["chembl_release"] = self._context.chembl_release
            return context

        def fetcher_factory(
            context: Mapping[str, Any],
        ) -> Callable[[Sequence[str] | None], Any]:
            strategy = self._fetcher_strategies.get(entity_name)
            if callable(strategy):
                fetcher = strategy(context, batch_plan)
                if callable(fetcher):
                    return self._wrap_fetcher(fetcher)
            msg = f"No fetcher strategy for entity: {entity_name}"
            raise ValueError(msg)

        def finalizer_factory(
            context: Mapping[str, Any],
        ) -> Callable[[Any], Any]:
            release = (
                context.get("chembl_release") or self._context.chembl_release
            )
            sort_columns = self._sort_fields.get(entity_name)

            def finalize(df: Any) -> Any:
                nonlocal release
                if release and "chembl_release" not in df.columns:
                    df = df.assign(chembl_release=release)
                columns_to_sort: Sequence[str] = (
                    list(sort_columns) if sort_columns else list(df.columns)
                )
                if df.empty:
                    return df
                return df.sort_values(
                    by=list(columns_to_sort),
                    ignore_index=True,
                )

            return finalize

        return ChemblExtractionServiceDescriptor[Any](
            build_context=build_context,
            fetcher_factory=fetcher_factory,
            finalizer_factory=finalizer_factory,
        )


__all__ = [
    "BatchPlan",
    "ChemblContextFacade",
    "ChemblDescriptorFactory",
    "FetcherStrategy",
]
