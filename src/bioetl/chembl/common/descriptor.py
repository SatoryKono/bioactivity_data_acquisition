"""Shared utilities and base class for ChEMBL pipelines.

This module provides common functionality for all ChEMBL-based pipelines,
including configuration resolution, API client management, pagination handling,
and data extraction utilities.
"""

# mypy: disable-error-code=misc
# Note: mypy errors about "erased type of self" in ChemblDescriptorBuilderMixin
# are suppressed because SelfDescriptorT is bound to ChemblPipelineBase, but
# methods are called on classes that inherit both ChemblPipelineBase and
# ChemblDescriptorBuilderMixin (e.g., UnifiedPipelineBase).

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Generic, Literal, Protocol, TypeVar, cast
from urllib.parse import urlparse

import pandas as pd
from structlog.stdlib import BoundLogger
from typing_extensions import Self

from bioetl.clients.base import (
    build_filters_payload,
    merge_select_fields,
    normalize_select_fields,
)
from bioetl.clients.chembl_entity_factory import (
    ChemblClientBundle,
    ChemblEntityClientFactory,
)
from bioetl.clients.client_chembl import _resolve_status_endpoint
from bioetl.config.models.source import SourceConfig
from bioetl.core import APIClientFactory
from bioetl.core.common import ChemblReleaseMixin
from bioetl.core.http import UnifiedAPIClient
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.core.pipeline import PipelineBase, PipelineExtractionMode
from bioetl.core.pipeline.errors import PipelineError
from bioetl.schemas import SchemaRegistryEntry
from bioetl.schemas.pipeline_contracts import get_out_schema

if TYPE_CHECKING:
    from bioetl.pipelines.mixins.descriptor_builder import (
        DescriptorStrategyFactory,
    )
else:
    DescriptorStrategyFactory = Any  # type: ignore[assignment, misc, unused-ignore]


class ChemblDescriptorPipelineProtocol(Protocol):
    """Minimal pipeline contract required by descriptor-driven extraction.

    The protocol is intentionally small and focuses on the behaviour exercised by
    descriptor builders and extraction helpers. Concrete implementations such as
    :class:`ChemblPipelineBase` provide a superset of this API.
    """

    config: Any
    pipeline_code: str
    id_column: str | None

    _registered_clients: dict[str, Any]

    def build_chembl_entity_bundle(
        self,
        entity_name: str,
        *,
        source_name: str = "chembl",
        source_config: SourceConfig[Any] | None = None,
        options: Mapping[str, Any] | None = None,
        chembl_client_kwargs: Mapping[str, Any] | None = None,
        fresh_http_client: bool = False,
    ) -> ChemblClientBundle: ...

    def register_client(self, name: str, client: UnifiedAPIClient | Any) -> None: ...

    # Methods required by descriptor-driven helpers. These mirror the
    # corresponding methods on ``ChemblPipelineBase`` but are intentionally
    # specified with broad ``Any`` types to avoid over-constraining callers.

    def _resolve_source_config(self, name: str) -> SourceConfig[Any]: ...

    def ensure_chembl_release(
        self,
        context: ChemblExtractionContext,
        log: BoundLogger,
    ) -> tuple[str | None, dict[str, Any]]: ...

    def _resolve_batch_size(self, source_config: SourceConfig[Any]) -> int: ...

    def _resolve_page_size(
        self,
        batch_size: int,
        limit: int | None,
        *,
        hard_cap: int = 25,
    ) -> int: ...

    def _normalize_parameters(self, parameters: Any) -> dict[str, Any]: ...

    def publish_release_metadata(
        self,
        payload: Mapping[str, Any] | None = None,
        *,
        release: str | None,
        metadata: Mapping[str, Any] | None = None,
        include_metadata: bool = True,
    ) -> dict[str, Any]: ...

    def record_extract_metadata(
        self,
        *,
        filters: Mapping[str, Any],
        requested_at_utc: datetime,
        **kwargs: Any,
    ) -> None: ...

    def _coerce_mapping(self, payload: Any) -> Mapping[str, Any]: ...

    @property
    def chembl_release(self) -> str | None: ...

    def _set_chembl_release(self, value: str | None) -> None: ...

    def run_batched_extraction(
        self,
        ids: Sequence[Any],
        *,
        id_column: str,
        fetcher: Callable[[Sequence[str], BatchExtractionContext], Any],
        select_fields: Sequence[str] | None = None,
        required_fields: Sequence[str] | None = None,
        limit: int | None = None,
        batch_size: int | None = None,
        chunk_size: int | None = None,
        max_batch_size: int | None = 25,
        metadata_filters: Mapping[str, Any] | None = None,
        chembl_release: str | None = None,
        id_normalizer: Callable[[Any], tuple[str | None, Any]] | None = None,
        sort_key: Callable[[tuple[str, Any]], Any] | None = None,
        transform_item: (
            Callable[[Mapping[str, Any], BatchExtractionContext], Mapping[str, Any]]
            | None
        ) = None,
        finalize: (
            Callable[[pd.DataFrame, BatchExtractionContext], pd.DataFrame]
            | None
        ) = None,
        finalize_context: (
            Callable[[BatchExtractionContext], None] | None
        ) = None,
        empty_frame_factory: Callable[[], pd.DataFrame] | None = None,
        stats_attribute: str | None = None,
        fetch_mode: Literal["default", "delegated"] = "default",
    ) -> tuple[pd.DataFrame, BatchExtractionStats]: ...


PipelineT = TypeVar(
    "PipelineT",
    bound="ChemblDescriptorPipelineProtocol",
    contravariant=True,
)  # noqa: UP037
FetcherCallable = Callable[
    [Sequence[str], "BatchExtractionContext"], Any
]  # noqa: UP037
FetcherFactory = Callable[
    ["ChemblExtractionContext", BoundLogger],
    FetcherCallable,
]  # noqa: UP037
FinalizeCallable = Callable[
    [pd.DataFrame, "BatchExtractionContext"],
    pd.DataFrame,
]  # noqa: UP037
FinalizeFactory = Callable[
    ["ChemblExtractionContext", BoundLogger],
    FinalizeCallable,
]  # noqa: UP037
FinalizeContextCallable = Callable[
    ["BatchExtractionContext"], None
]  # noqa: UP037
FinalizeContextFactory = Callable[
    ["ChemblExtractionContext", BoundLogger],
    FinalizeContextCallable,
]  # noqa: UP037
DryRunHandler = Callable[
    [ChemblDescriptorPipelineProtocol, "ChemblExtractionContext", BoundLogger],
    pd.DataFrame,
]  # noqa: UP037
SummaryFactory = Callable[
    ["ChemblExtractionContext", "BatchExtractionStats"],
    Mapping[str, Any],
]  # noqa: UP037


@dataclass(slots=True)
class ChemblContextSpec(Generic[PipelineT]):
    """Declarative configuration for building :class:`ChemblExtractionContext`."""

    entity_name: str
    entity_client_type: type[Any] | None = None
    release_resolver: (
        Callable[[PipelineT, Any, BoundLogger, Any | None], str | None] | None
    ) = None
    select_fields_resolver: (
        Callable[[PipelineT, Any], Sequence[str] | None] | None
    ) = None
    extra_filters_factory: (
        Callable[[Any, PipelineT], dict[str, Any]] | None
    ) = None
    client_registry_name: str | Callable[[PipelineT], str | None] | None = None
    chembl_release_override: str | Callable[[PipelineT], str | None] | None = (
        None
    )
    page_size_resolver: Callable[[Any], int | None] | None = None
    pre_release_hook: Callable[[PipelineT, Any, Any], None] | None = None
    after_build: (
        Callable[
            [PipelineT, ChemblExtractionContext, Any, BoundLogger],
            ChemblExtractionContext,
        ]
        | None
    ) = None
    builder: (
        Callable[[PipelineT, Any, BoundLogger], ChemblExtractionContext] | None
    ) = None


@dataclass(slots=True)
class ChemblDescriptorSpec(Generic[PipelineT]):
    """Declarative descriptor schema consumed by :class:`ChemblDescriptorBuilderMixin`."""

    name: str
    source_name: str
    source_config_factory: Callable[[SourceConfig[Any]], Any]
    context: ChemblContextSpec[PipelineT]
    id_column: str
    summary_event: str
    must_have_fields: Sequence[str] = ()
    default_select_fields: Sequence[str] | None = None
    record_transform: (
        Callable[
            [PipelineT, Mapping[str, Any], ChemblExtractionContext],
            Mapping[str, Any],
        ]
        | None
    ) = None
    post_processors: Sequence[
        Callable[
            [PipelineT, pd.DataFrame, ChemblExtractionContext, BoundLogger],
            pd.DataFrame,
        ]
    ] = ()
    sort_by: Sequence[str] | str | None = None
    empty_frame_factory: (
        Callable[[PipelineT, ChemblExtractionContext], pd.DataFrame] | None
    ) = None
    dry_run_handler: (
        Callable[
            [PipelineT, ChemblExtractionContext, BoundLogger, float],
            pd.DataFrame,
        ]
        | None
    ) = None
    hard_page_size_cap: int | None = 25
    summary_extra: (
        Callable[
            [PipelineT, pd.DataFrame, ChemblExtractionContext],
            Mapping[str, Any],
        ]
        | None
    ) = None


SelfDescriptorT = TypeVar(
    "SelfDescriptorT",
    bound=ChemblDescriptorPipelineProtocol,
)  # noqa: UP037


@dataclass(slots=True)
class ChemblExtractionContext:
    """Holds runtime state for a descriptor-driven extraction run."""

    source_config: Any
    iterator: Any
    chembl_client: Any | None = None
    select_fields: Sequence[str] | None = None
    page_size: int | None = None
    chembl_release: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    extra_filters: dict[str, Any] = field(default_factory=dict)
    iterate_all_kwargs: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)
    release_resolver: (
        Callable[
            [ChemblPipelineBase, Any, BoundLogger, Any | None], str | None
        ]
        | None
    ) = None


def build_standard_chembl_context(
    pipeline: ChemblPipelineBase,
    entity_name: str,
    source_config: Any,
    log: BoundLogger,
    *,
    entity_client_type: type[Any] | None = None,
    release_resolver: (
        Callable[
            [ChemblPipelineBase, Any, BoundLogger, Any | None], str | None
        ]
        | None
    ) = None,
    select_fields_resolver: (
        Callable[[ChemblPipelineBase, Any], Sequence[str] | None] | None
    ) = None,
    extra_filters_factory: (
        Callable[[Any, ChemblPipelineBase], dict[str, Any]] | None
    ) = None,
    client_registry_name: str | None = None,
    chembl_release_override: str | None = None,
    page_size_resolver: Callable[[Any], int | None] | None = None,
    pre_release_hook: (
        Callable[[ChemblPipelineBase, Any, Any], None] | None
    ) = None,
) -> ChemblExtractionContext:
    """Унифицированная фабрика для создания ChemblExtractionContext.

    Инкапсулирует общую логику создания контекста извлечения для ChEMBL пайплайнов:
    создание bundle, регистрация клиента, получение release, создание контекста.

    Parameters
    ----------
    pipeline
        Экземпляр пайплайна, наследующий ChemblPipelineBase.
    entity_name
        Имя сущности ("target", "testitem", "assay" и т.д.).
    source_config
        Конфигурация источника данных.
    log
        Логгер для записи событий.
    entity_client_type
        Ожидаемый тип entity_client для проверки. Если None, проверка не выполняется.
    release_resolver
        Функция для получения ChEMBL release.
        Принимает (pipeline, chembl_client, log, entity_client)
        и передается в контекст для
        отложенного вызова :meth:`ChemblPipelineBase.ensure_chembl_release`.
        При ``None`` используется стандартный ``pipeline.resolve_chembl_release``.
    pre_release_hook
        Функция, вызываемая перед получением release (например, для handshake).
        Принимает (pipeline, source_config, entity_client).
    select_fields_resolver
        Функция для получения списка полей. По умолчанию используется
        `source_config.parameters.select_fields`.
    extra_filters_factory
        Функция для создания extra_filters. Должна принимать source_config и pipeline,
        возвращать dict. По умолчанию возвращает пустой dict.
    client_registry_name
        Имя для регистрации HTTP-клиента. По умолчанию `f"chembl_{entity_name}_http"`.
    chembl_release_override
        Переопределение значения chembl_release в контексте (например, для testitem
        используется chembl_db_version). Если None, release будет определен позднее через
        :meth:`ChemblPipelineBase.ensure_chembl_release`.
    page_size_resolver
        Функция для получения page_size. По умолчанию используется
        `getattr(source_config, "page_size", None)`.

    Returns
    -------
    ChemblExtractionContext
        Созданный контекст извлечения.

    Raises
    ------
    RuntimeError
        Если entity_client не найден в bundle или имеет неверный тип.
    """
    _ = log
    # Создаем bundle
    bundle = pipeline.build_chembl_entity_bundle(
        entity_name,
        source_name="chembl",
        source_config=source_config,
        options=None,
        chembl_client_kwargs=None,
        fresh_http_client=False,
    )

    # Регистрируем HTTP-клиент
    registry_name = client_registry_name or f"chembl_{entity_name}_http"
    if registry_name not in pipeline._registered_clients:
        pipeline.register_client(registry_name, bundle.api_client)

    chembl_client = bundle.chembl_client

    # Получаем entity_client и проверяем
    entity_client = bundle.entity_client
    if entity_client is None:
        msg = f"Фабрика вернула пустой клиент для '{entity_name}'"
        raise RuntimeError(msg)
    # Проверяем тип только если entity_client_type является реальным типом (не моком)
    if (
        entity_client_type is not None
        and isinstance(entity_client_type, type)
        and not isinstance(entity_client, entity_client_type)
    ):
        msg = f"Ожидался {entity_client_type.__name__}, получен {type(entity_client).__name__}"
        raise RuntimeError(msg)

    # Выполняем pre_release_hook если нужно (например, для handshake)
    if pre_release_hook is not None:
        pre_release_hook(pipeline, source_config, entity_client)

    # Получаем select_fields
    if select_fields_resolver is not None:
        select_fields = select_fields_resolver(pipeline, source_config)
    else:
        # По умолчанию из source_config.parameters.select_fields
        select_fields = getattr(source_config, "parameters", None)
        if select_fields is not None:
            select_fields = getattr(select_fields, "select_fields", None)

    # Получаем page_size
    if page_size_resolver is not None:
        page_size = page_size_resolver(source_config)
    else:
        page_size = getattr(source_config, "page_size", None)

    # Создаем extra_filters
    if extra_filters_factory is not None:
        extra_filters = extra_filters_factory(source_config, pipeline)
    else:
        extra_filters = {}

    # Определяем значение для chembl_release в контексте
    context_release = chembl_release_override

    # Создаем контекст
    return ChemblExtractionContext(
        source_config,
        entity_client,
        chembl_client,
        list(select_fields) if select_fields else None,
        page_size,
        context_release,
        extra_filters=extra_filters,
        release_resolver=release_resolver,
    )


@dataclass(slots=True)
class ChemblExtractionDescriptor(Generic[PipelineT]):
    """Descriptor describing how to execute a ``run_extract_all`` operation."""

    name: str
    source_name: str
    source_config_factory: Callable[[SourceConfig[Any]], Any]
    build_context: Callable[
        [PipelineT, Any, BoundLogger], ChemblExtractionContext
    ]
    id_column: str
    summary_event: str
    must_have_fields: tuple[str, ...] = ()
    default_select_fields: tuple[str, ...] | None = None
    record_transform: (
        Callable[
            [PipelineT, Mapping[str, Any], ChemblExtractionContext],
            Mapping[str, Any],
        ]
        | None
    ) = None
    post_processors: Sequence[
        Callable[
            [PipelineT, pd.DataFrame, ChemblExtractionContext, BoundLogger],
            pd.DataFrame,
        ]
    ] = ()
    sort_by: Sequence[str] | str | None = None
    empty_frame_factory: (
        Callable[[PipelineT, ChemblExtractionContext], pd.DataFrame] | None
    ) = None
    dry_run_handler: (
        Callable[
            [PipelineT, ChemblExtractionContext, BoundLogger, float],
            pd.DataFrame,
        ]
        | None
    ) = None
    hard_page_size_cap: int | None = 25
    summary_extra: (
        Callable[
            [PipelineT, pd.DataFrame, ChemblExtractionContext],
            Mapping[str, Any],
        ]
        | None
    ) = None


class ChemblDescriptorBuilderMixin(
    Generic[PipelineT]
):  # pyright: ignore[reportInvalidTypeArguments, reportArgumentType]
    """Mixin providing declarative descriptor construction for Chembl pipelines.

    Note: mypy errors about "erased type of self" are suppressed because SelfDescriptorT
    is bound to ChemblPipelineBase, but methods are called on classes that inherit both
    ChemblPipelineBase and ChemblDescriptorBuilderMixin (e.g., UnifiedPipelineBase).
    """

    def descriptor_spec(
        self: SelfDescriptorT,
    ) -> ChemblDescriptorSpec[
        SelfDescriptorT
    ]:  # pyright: ignore[reportInvalidTypeArguments, reportGeneralTypeIssues]; type: ignore
        """Return the specification consumed by :meth:`build_descriptor`."""

        msg = f"{type(self).__name__} must implement descriptor_spec()"
        raise NotImplementedError(msg)

    def build_descriptor(
        self: SelfDescriptorT,
    ) -> ChemblExtractionDescriptor[
        SelfDescriptorT
    ]:  # pyright: ignore[reportInvalidTypeArguments, reportGeneralTypeIssues]; type: ignore
        """Construct a descriptor instance based on :meth:`descriptor_spec`."""

        # pyright: ignore[reportCallWithNoReturn]
        # pylint: disable=assignment-from-no-return
        # type-checker comments moved above the call
        spec = self.descriptor_spec()  # type: ignore[attr-defined]

        must_have = tuple(spec.must_have_fields or ())

        if not spec.id_column:
            msg = f"Descriptor '{spec.name}' must define a non-empty id_column"
            raise PipelineError(msg)

        if must_have and spec.id_column not in must_have:
            msg = (
                f"Descriptor '{spec.name}' must include id_column "
                f"'{spec.id_column}' in must_have_fields; got {must_have!r}"
            )
            raise PipelineError(msg)

        default_select: tuple[str, ...] | None
        if spec.default_select_fields is None:
            default_select = None
        else:
            default_select = tuple(str(col) for col in spec.default_select_fields)
            if not default_select:
                msg = (
                    f"Descriptor '{spec.name}' defines empty "
                    "default_select_fields; leave it as None to disable "
                    "defaults"
                )
                raise PipelineError(msg)

        post_processors = tuple(spec.post_processors or ())

        raw_sort_by = spec.sort_by
        sort_by: tuple[str, ...] | None
        if raw_sort_by is None:
            sort_by = None
        elif isinstance(raw_sort_by, Sequence) and not isinstance(
            raw_sort_by, (str, bytes)
        ):
            sort_by = tuple(str(col) for col in raw_sort_by)
        else:
            sort_by = (str(raw_sort_by),)

        hard_cap = spec.hard_page_size_cap
        if hard_cap is not None and hard_cap <= 0:
            msg = (
                f"Descriptor '{spec.name}' has invalid hard_page_size_cap="
                f"{hard_cap}; expected a positive integer or None."
            )
            raise PipelineError(msg)

        build_context = self._build_context_callable(spec)  # type: ignore[attr-defined]
        empty_frame_factory = (
            spec.empty_frame_factory
            or self._default_empty_frame_factory(  # type: ignore[attr-defined]
                spec.id_column
            )
        )

        return ChemblExtractionDescriptor[
            SelfDescriptorT](
            name=spec.name,
            source_name=spec.source_name,
            source_config_factory=spec.source_config_factory,
            build_context=build_context,  # pyright: ignore[reportArgumentType]
            id_column=spec.id_column,
            summary_event=spec.summary_event,
            must_have_fields=must_have,
            default_select_fields=default_select,
            record_transform=spec.record_transform,
            post_processors=post_processors,
            sort_by=sort_by,
            empty_frame_factory=empty_frame_factory,  # pyright: ignore[reportArgumentType]
            dry_run_handler=spec.dry_run_handler,
            summary_extra=spec.summary_extra,
            hard_page_size_cap=hard_cap,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_context_callable(  # type: ignore
        self: SelfDescriptorT,  # pyright: ignore[reportGeneralTypeIssues]
        spec: ChemblDescriptorSpec[
            SelfDescriptorT
        ],  # pyright: ignore[reportInvalidTypeArguments]; type: ignore
    ) -> Callable[
        [ChemblPipelineBase, Any, BoundLogger], ChemblExtractionContext
    ]:
        # pyright: ignore[reportAttributeAccessIssue]
        context_spec = spec.context
        typed_self_any = cast(Any, self)

        def build_context(
            pipeline: ChemblPipelineBase,
            source_config: Any,
            log: BoundLogger,
        ) -> ChemblExtractionContext:
            typed_pipeline = cast(SelfDescriptorT, pipeline)

            if context_spec.builder is not None:
                context = context_spec.builder(
                    typed_pipeline, source_config, log
                )
            else:
                release_resolver = typed_self_any._wrap_release_resolver(
                    context_spec
                )  # type: ignore[attr-defined]
                select_fields_resolver = (
                    typed_self_any._wrap_select_fields_resolver(
                        context_spec
                    )
                )  # type: ignore[attr-defined]
                extra_filters_factory = (
                    typed_self_any._wrap_extra_filters_factory(context_spec)
                )  # type: ignore[attr-defined]
                pre_release_hook = typed_self_any._wrap_pre_release_hook(
                    context_spec
                )  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]
                client_registry_name = self._resolve_context_value(  # type: ignore[attr-defined]
                    context_spec.client_registry_name,
                    typed_pipeline,
                )
                chembl_release_override = self._resolve_context_value(  # type: ignore[attr-defined]
                    context_spec.chembl_release_override,
                    typed_pipeline,
                )

                context = build_standard_chembl_context(
                    cast(ChemblPipelineBase, typed_pipeline),
                    context_spec.entity_name,
                    source_config,
                    log,
                    entity_client_type=context_spec.entity_client_type,
                    release_resolver=release_resolver,
                    select_fields_resolver=select_fields_resolver,
                    extra_filters_factory=extra_filters_factory,
                    client_registry_name=client_registry_name,
                    chembl_release_override=chembl_release_override,
                    page_size_resolver=context_spec.page_size_resolver,
                    pre_release_hook=pre_release_hook,
                )

            if context_spec.after_build is not None:
                context = context_spec.after_build(
                    typed_pipeline,
                    context,
                    source_config,
                    log,
                )

            return context

        return build_context

    def _wrap_release_resolver(  # type: ignore
        self: SelfDescriptorT,  # pyright: ignore[reportGeneralTypeIssues]
        context_spec: ChemblContextSpec[
            SelfDescriptorT
        ],  # pyright: ignore[reportInvalidTypeArguments]; type: ignore
    ) -> (
        Callable[
            [ChemblPipelineBase, Any, BoundLogger, Any | None], str | None
        ]
        | None
    ):
        resolver = context_spec.release_resolver
        if resolver is None:
            return None

        def wrapper(
            pipeline: ChemblPipelineBase,
            client: Any,
            log: BoundLogger,
            entity_client: Any | None,
        ) -> str | None:
            typed_pipeline = cast(
                SelfDescriptorT, pipeline
            )  # pyright: ignore[reportInvalidTypeArguments]
            return resolver(typed_pipeline, client, log, entity_client)

        return wrapper

    def _wrap_select_fields_resolver(  # type: ignore
        self: SelfDescriptorT,  # pyright: ignore[reportGeneralTypeIssues]
        context_spec: ChemblContextSpec[
            SelfDescriptorT
        ],  # pyright: ignore[reportInvalidTypeArguments]; type: ignore[type-arg]
    ) -> Callable[[ChemblPipelineBase, Any], Sequence[str] | None] | None:
        resolver = context_spec.select_fields_resolver
        if resolver is None:
            return None

        def wrapper(
            pipeline: ChemblPipelineBase, source_config: Any
        ) -> Sequence[str] | None:
            typed_pipeline = cast(
                SelfDescriptorT, pipeline
            )  # pyright: ignore[reportInvalidTypeArguments]
            return resolver(typed_pipeline, source_config)

        return wrapper

    def _wrap_extra_filters_factory(  # type: ignore
        self: SelfDescriptorT,  # pyright: ignore[reportGeneralTypeIssues]
        context_spec: ChemblContextSpec[
            SelfDescriptorT
        ],  # pyright: ignore[reportInvalidTypeArguments]; type: ignore[type-arg]
    ) -> Callable[[Any, ChemblPipelineBase], dict[str, Any]] | None:
        factory = context_spec.extra_filters_factory
        if factory is None:
            return None

        def wrapper(
            source_config: Any, pipeline: ChemblPipelineBase
        ) -> dict[str, Any]:
            typed_pipeline = cast(
                SelfDescriptorT, pipeline
            )  # pyright: ignore[reportInvalidTypeArguments]
            return factory(source_config, typed_pipeline)

        return wrapper

    def _wrap_pre_release_hook(  # type: ignore
        self: SelfDescriptorT,  # pyright: ignore[reportGeneralTypeIssues]
        context_spec: ChemblContextSpec[
            SelfDescriptorT
        ],  # pyright: ignore[reportInvalidTypeArguments]; type: ignore[type-arg]
    ) -> Callable[[ChemblPipelineBase, Any, Any], None] | None:
        hook = context_spec.pre_release_hook
        if hook is None:
            return None

        def wrapper(
            pipeline: ChemblPipelineBase,
            source_config: Any,
            entity_client: Any,
        ) -> None:
            typed_pipeline = cast(
                SelfDescriptorT, pipeline
            )  # pyright: ignore[reportInvalidTypeArguments]
            hook(typed_pipeline, source_config, entity_client)

        return wrapper

    @staticmethod
    def _resolve_context_value(
        candidate: str | Callable[[SelfDescriptorT], str | None] | None,
        pipeline: SelfDescriptorT,
    ) -> str | None:
        if candidate is None:
            return None
        if callable(candidate):
            return candidate(pipeline)
        return candidate

    @staticmethod
    def _default_empty_frame_factory(
        id_column: str,
    ) -> Callable[[ChemblPipelineBase, ChemblExtractionContext], pd.DataFrame]:
        def empty_frame(
            _: ChemblPipelineBase,
            __: ChemblExtractionContext,
        ) -> pd.DataFrame:
            return pd.DataFrame({id_column: pd.Series(dtype="string")})

        return empty_frame


@dataclass(slots=True)
class BatchExtractionStats:
    """Summary information generated by :meth:`run_batched_extraction`."""

    requested: int
    rows: int = 0
    batches: int = 0
    api_calls: int | None = None
    cache_hits: int | None = None
    duration_ms: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """Return a dictionary representation suitable for persistence/logging."""

        payload: dict[str, Any] = {
            "requested": self.requested,
            "rows": self.rows,
            "batches": self.batches,
        }
        if self.api_calls is not None:
            payload["api_calls"] = self.api_calls
        if self.cache_hits is not None:
            payload["cache_hits"] = self.cache_hits
        if self.duration_ms is not None:
            payload["duration_ms"] = self.duration_ms
        if self.extra:
            payload.update(self.extra)
        return payload

    def for_logging(self) -> dict[str, Any]:
        """Return keyword arguments appropriate for structured logging."""

        return self.as_dict()

    def set_extra(self, **kwargs: Any) -> None:
        """Merge additional diagnostic metadata into the summary payload."""

        if kwargs:
            self.extra.update(kwargs)


@dataclass(slots=True)
class BatchExtractionContext:
    """Shared state passed to hooks during batch extraction."""

    ids: tuple[str, ...]
    id_column: str
    select_fields: tuple[str, ...]
    limit: int | None
    batch_size: int
    chunk_size: int
    stats: BatchExtractionStats
    log: BoundLogger
    metadata: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    def increment_batches(self) -> None:
        """Increment processed batch counter."""

        self.stats.batches += 1

    def increment_api_calls(self, *, count: int = 1) -> None:
        """Increment API call counter for diagnostics."""

        if self.stats.api_calls is None:
            self.stats.api_calls = 0
        self.stats.api_calls += count

    def set_cache_hits(self, value: int | None) -> None:
        """Record cache hit counter for diagnostics."""

        self.stats.cache_hits = value

    def set_extra(self, **kwargs: Any) -> None:
        """Merge arbitrary diagnostic metadata into the stats payload."""

        if kwargs:
            self.stats.extra.update(kwargs)


# ChemblClient is dynamically loaded in __init__.py at runtime
# Type checking uses Any for client parameters to avoid circular dependencies


class ChemblPipelineBase(ChemblReleaseMixin, PipelineBase):
    """Base class for ChEMBL-based ETL pipelines.

    This class provides common functionality for all ChEMBL pipelines,
    including configuration resolution, API client management, pagination,
    and data extraction utilities.
    """

    #: Canonical identifier column used when reading CLI-supplied input files.
    id_column: str | None = None

    #: Structured logging event emitted when the extraction mode is resolved.
    extract_event_name: str | None = None

    #: Source label used when identifiers are provided via legacy hooks.
    legacy_extract_source: str = "legacy"

    def build_descriptor(self: Self) -> ChemblExtractionDescriptor[Self]:
        """Return the descriptor used by :meth:`extract_all`."""

        msg = f"{type(self).__name__} must implement build_descriptor()"
        raise NotImplementedError(msg)

    def extract_all(self) -> pd.DataFrame:
        """Extract all records according to the pipeline descriptor."""

        # pyright: ignore[reportCallWithNoReturn]
        # pylint: disable=assignment-from-no-return
        descriptor = self.build_descriptor()
        return self.run_extract_all(descriptor)

    def __init__(self, config: Any, run_id: str) -> None:
        """Initialize the ChEMBL pipeline base.

        Parameters
        ----------
        config
            Pipeline configuration object.
        run_id
            Unique identifier for this pipeline run.
        """
        super().__init__(config, run_id)
        self._client_factory = APIClientFactory(config)
        self._chembl_entity_factory = ChemblEntityClientFactory(
            config,
            api_client_factory=self._client_factory,
        )
        self._api_version: str | None = None
        self._output_schema_entry: SchemaRegistryEntry | None = None
        # Use a broad type for _output_schema to accommodate both the
        # deprecated ``pandera.DataFrameSchema`` shim and the
        # ``pandera.api.pandas.DataFrameSchema`` implementation used by
        # SchemaRegistryEntry.schema.
        self._output_schema: Any | None = None
        self._output_column_order: tuple[str, ...] = ()
        self._output_schema_cache: dict[str, Any] = {}
        self._chembl_release_metadata: dict[str, Any] = {}
        self._descriptor_strategy_factory: DescriptorStrategyFactory | None = (
            None  # pyright: ignore[reportInvalidTypeArguments]
        )

        # Initialize output schema based on pipeline_code
        from contextlib import suppress

        with suppress(KeyError, AttributeError):
            # If schema cannot be resolved (e.g., pipeline_code not set yet), defer initialization
            self.initialize_output_schema()

    def configure_output_schema(
        self,
        schema_entry: SchemaRegistryEntry,
        *,
        extra_cache: dict[str, Any] | None = None,
    ) -> None:
        """Configure runtime caches bound to the pipeline output schema."""

        self._output_schema_entry = schema_entry
        self._output_schema = cast(Any, schema_entry.schema)
        self._output_column_order = tuple(schema_entry.column_order)
        self._output_schema_cache = (
            dict(extra_cache) if extra_cache is not None else {}
        )

    def get_descriptor_strategy_factory(self) -> Any:
        """Return (and lazily construct) the batching strategy factory."""

        if self._descriptor_strategy_factory is None:
            from bioetl.pipelines.mixins.descriptor_builder import (
                DescriptorStrategyFactory,
            )

            self._descriptor_strategy_factory = DescriptorStrategyFactory()
        return self._descriptor_strategy_factory

    def resolve_output_schema_entry(self) -> SchemaRegistryEntry:
        """Return the schema registry entry associated with this pipeline.

        Subclasses may override this method when they need to fetch the
        registry entry from a non-standard location. The default
        implementation consults :func:`bioetl.schemas.pipeline_contracts.get_out_schema`
        using the pipeline actor (when available) and finally falls back to the
        configured :attr:`pipeline_code`.
        """

        candidates: list[str] = []

        actor = getattr(self, "actor", None)
        actor_code = (
            stripped
            if isinstance(actor, str) and (stripped := actor.strip())
            else None
        )

        pipeline_code = self.pipeline_code.strip()

        if actor_code and actor_code != pipeline_code:
            candidates.append(actor_code)

        candidates.append(pipeline_code)

        last_error: KeyError | None = None

        for candidate in candidates:
            try:
                return get_out_schema(candidate)
            except KeyError as exc:
                last_error = exc
                continue

        if last_error is not None:
            raise last_error

        msg = "Unable to resolve pipeline output schema"
        raise KeyError(msg)

    def initialize_output_schema(
        self,
        schema_entry: SchemaRegistryEntry | None = None,
        *,
        extra_cache: dict[str, Any] | None = None,
    ) -> None:
        """Resolve and configure the pipeline output schema.

        Parameters
        ----------
        schema_entry
            Optional pre-resolved schema registry entry. When omitted the
            registry entry is obtained via :meth:`resolve_output_schema_entry`.
        extra_cache
            Optional mapping propagated to :meth:`configure_output_schema` for
            callers that need to seed schema-level caches.
        """

        resolved_entry = schema_entry or self.resolve_output_schema_entry()
        self.configure_output_schema(resolved_entry, extra_cache=extra_cache)

    # ------------------------------------------------------------------
    # Normalization helpers
    # ------------------------------------------------------------------

    def _normalize_identifiers(
        self, df: pd.DataFrame, log: BoundLogger
    ) -> pd.DataFrame:
        """Normalize identifier columns.

        Subclasses are expected to override this method with domain specific
        logic. The default implementation is a no-op so tests can provide
        lightweight pipeline stubs without having to implement normalization.
        """

        _ = log
        return df

    def _normalize_string_fields(
        self, df: pd.DataFrame, log: BoundLogger
    ) -> pd.DataFrame:
        """Normalize free-text columns.

        Subclasses are expected to override this method. The default
        implementation is intentionally a no-op to avoid forcing all call sites
        to provide custom behaviour.
        """

        _ = log
        return df

    def _normalize_and_enforce_schema(
        self,
        df: pd.DataFrame,
        column_order: Sequence[str],
        log: BoundLogger,
        *,
        normalize_identifiers: bool = True,
        normalize_strings: bool = True,
        order_columns: bool = False,
        copy: bool = True,
    ) -> pd.DataFrame:
        """Apply shared normalization steps and ensure schema columns.

        Parameters
        ----------
        df
            DataFrame to normalize.
        column_order
            Target schema column order used for enforcement and optional
            ordering.
        log
            Logger used for diagnostic output.
        normalize_identifiers
            When True (default) invoke :meth:`_normalize_identifiers`.
        normalize_strings
            When True (default) invoke :meth:`_normalize_string_fields`.
        order_columns
            When True reorder columns using :meth:`_order_schema_columns`.
        copy
            Control whether the DataFrame should be copied before mutation.

        Returns
        -------
        pd.DataFrame
            Normalized DataFrame with all schema columns present.
        """

        working_df = df.copy() if copy else df

        working_df = self._ensure_schema_columns(working_df, column_order, log)

        if normalize_identifiers:
            working_df = self._normalize_identifiers(working_df, log)

        if normalize_strings:
            working_df = self._normalize_string_fields(working_df, log)

        working_df = self._ensure_schema_columns(working_df, column_order, log)

        if order_columns:
            working_df = self._order_schema_columns(working_df, column_order)

        return working_df

    @property
    def api_version(self) -> str | None:
        """Return the cached API version captured during extraction."""
        return self._get_optional_string_value(
            "_api_version", field_name="api_version"
        )

    def _set_api_version(self, value: str | None) -> None:
        """Update the cached API version used by the pipeline."""
        self._set_optional_string_value(
            "_api_version", value, field_name="api_version"
        )
        self.update_chembl_release_metadata(api_version=value)

    def extract(
        self,
        *,
        mode: PipelineExtractionMode = PipelineExtractionMode.AUTO,
        ids: Sequence[str] | None = None,
        **legacy_kwargs: object,
    ) -> pd.DataFrame:
        """Dispatch between batch and full extraction modes."""

        log = UnifiedLogger.get(__name__).bind(
            component=self._component_for_stage("extract")
        )
        entity_raw = getattr(self, "entity_name", "") or ""
        entity_key = str(entity_raw).strip().lower()
        default_event = (
            f"chembl_{entity_key}.extract_mode"
            if entity_key
            else f"{self.pipeline_code}.extract_mode"
        )
        event_name = self.extract_event_name or default_event
        id_column_name = self.id_column or self._get_id_column_name()
        normalized_ids: tuple[str, ...] = (
            tuple(str(item) for item in ids) if ids else ()
        )

        if mode is PipelineExtractionMode.BATCH:
            if not normalized_ids:
                msg = "batch extraction requires non-empty ids"
                raise PipelineError(msg)
            source = (
                "cli_input"
                if getattr(self.config.cli, "input_file", None)
                else "runtime"
            )
            log.info(
                event_name,
                mode="batch",
                source=source,
                ids_count=len(normalized_ids),
            )
            return self.extract_by_ids(normalized_ids)

        if mode is PipelineExtractionMode.FULL:
            log.info(event_name, mode="full")
            return self.extract_all()

        if normalized_ids:
            source = (
                "cli_input"
                if getattr(self.config.cli, "input_file", None)
                else "runtime"
            )
            log.info(
                event_name,
                mode="batch",
                source=source,
                ids_count=len(normalized_ids),
            )
            return self.extract_by_ids(normalized_ids)

        legacy_resolver: (
            Callable[[BoundLogger], Sequence[str] | None] | None
        ) = None
        if legacy_kwargs and not self.has_legacy_extract_support():
            unexpected = ", ".join(sorted(legacy_kwargs))
            msg = (
                f"extract() received unsupported keyword arguments: {unexpected}. "
                "Provide identifiers via --input-file."
            )
            raise TypeError(msg)

        if self.has_legacy_extract_support():

            def _legacy(bound_log: BoundLogger) -> Sequence[str] | None:
                return self.resolve_legacy_extract_ids(
                    bound_log, **legacy_kwargs
                )

            legacy_resolver = _legacy

        return self._dispatch_extract_mode(
            log,
            event_name=event_name,
            batch_callback=self.extract_by_ids,
            full_callback=self.extract_all,
            id_column_name=id_column_name,
            legacy_id_resolver=legacy_resolver,
            legacy_source=self.legacy_extract_source,
        )

    def has_legacy_extract_support(self) -> bool:
        """Return whether the pipeline exposes a legacy ID resolver."""

        return (
            type(self).resolve_legacy_extract_ids
            is not ChemblPipelineBase.resolve_legacy_extract_ids
        )

    def resolve_legacy_extract_ids(
        self,
        log: BoundLogger,
        *args: object,
        **kwargs: object,
    ) -> Sequence[str] | None:
        """Resolve identifiers supplied via deprecated inputs."""

        _ = (log, args, kwargs)
        return None

    # ------------------------------------------------------------------
    # Configuration resolution methods
    # ------------------------------------------------------------------

    def _resolve_source_config(self, name: str) -> SourceConfig[Any]:
        """Resolve source configuration by name.

        Parameters
        ----------
        name
            Name of the source configuration to resolve.

        Returns
        -------
        SourceConfig
            The resolved source configuration.

        Raises
        ------
        KeyError
            If the source is not configured for this pipeline.
        """
        try:
            return cast(SourceConfig[Any], self.config.sources[name])
        except KeyError as exc:
            msg = f"Source '{name}' is not configured for pipeline '{self.pipeline_code}'"
            raise KeyError(msg) from exc

    @staticmethod
    def _stringify_mapping(mapping: Mapping[object, Any]) -> dict[str, Any]:
        """Return mapping with stringified keys preserving values."""

        return {str(key): value for key, value in mapping.items()}

    @staticmethod
    def _normalize_parameters(parameters: Any) -> dict[str, Any]:
        """Return parameters as a plain mapping.

        Parameters
        ----------
        parameters:
            Source configuration parameters represented as Mapping, Pydantic
            model, or arbitrary object with attributes.

        Returns
        -------
        dict[str, Any]
            Normalised mapping with stringified keys preserving deterministic
            ordering semantics for later processing.
        """
        parameters_mapping = getattr(parameters, "parameters_mapping", None)
        if callable(parameters_mapping):
            mapping_candidate = parameters_mapping()
            if isinstance(mapping_candidate, Mapping):
                return {
                    str(key): value for key, value in mapping_candidate.items()
                }

        if isinstance(parameters, Mapping):
            mapping = cast(Mapping[object, Any], parameters)
            return {str(key): value for key, value in mapping.items()}

        model_dump = getattr(parameters, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump()
            if isinstance(dumped, Mapping):
                mapping = cast(Mapping[object, Any], dumped)
                return {str(key): value for key, value in mapping.items()}

        as_dict = getattr(parameters, "dict", None)
        if callable(as_dict):
            dumped = as_dict()
            if isinstance(dumped, Mapping):
                mapping = cast(Mapping[object, Any], dumped)
                return {str(key): value for key, value in mapping.items()}

        attrs = getattr(parameters, "__dict__", None)
        if isinstance(attrs, dict):
            attr_mapping = cast(dict[str, Any], attrs)
            return {
                key: value
                for key, value in attr_mapping.items()
                if not key.startswith("_")
            }

        return {}

    @staticmethod
    def _resolve_base_url(parameters: Any) -> str:
        """Resolve base URL from source configuration parameters.

        Parameters
        ----------
        parameters
            Source configuration parameters mapping.

        Returns
        -------
        str
            The resolved base URL, normalized (trailing slash removed).

        Raises
        ------
        ValueError
            If base_url is not a non-empty string.
        """
        params = ChemblPipelineBase._normalize_parameters(parameters)
        base_url = (
            params.get("base_url") or "https://www.ebi.ac.uk/chembl/api/data"
        )
        if not isinstance(base_url, str) or not base_url.strip():
            msg = (
                "sources.chembl.parameters.base_url must be a non-empty string"
            )
            raise ValueError(msg)
        return base_url.rstrip("/")

    @staticmethod
    def _resolve_page_size(
        batch_size: int, limit: int | None, *, hard_cap: int = 25
    ) -> int:
        """Return deterministic page size respecting limit and API hard cap."""

        effective = min(max(int(batch_size), 1), hard_cap)
        if limit is not None:
            effective = min(effective, max(int(limit), 0))
        return max(effective, 1)

    @staticmethod
    def _resolve_batch_size(source_config: SourceConfig[Any]) -> int:
        """Resolve batch size from source configuration.

        Parameters
        ----------
        source_config
            Source configuration object.

        Returns
        -------
        int
            The resolved batch size (default: 25).
        """
        batch_size: int | None = getattr(source_config, "batch_size", None)
        if batch_size is None:
            parameters_mapping = source_config.parameters_mapping()
            candidate: Any = parameters_mapping.get("batch_size")
            if isinstance(candidate, int) and candidate > 0:
                batch_size = candidate
        if batch_size is None or batch_size <= 0:
            batch_size = 25
        return batch_size

    def _resolve_select_fields(
        self,
        source_config: SourceConfig[Any],
        default_fields: Sequence[str] | None = None,
    ) -> list[str]:
        """Resolve select_fields from config or use default.

        Parameters
        ----------
        source_config
            Source configuration object.
        default_fields
            Optional default field list to use if not configured.

        Returns
        -------
        list[str]
            List of field names to select from the API.
        """
        parameters = source_config.parameters_mapping()
        normalized = normalize_select_fields(
            parameters.get("select_fields"),
            default=default_fields,
        )
        if normalized is None:
            return []
        return list(normalized)

    @staticmethod
    def _merge_select_fields(
        select_fields: Sequence[str] | None,
        required_fields: Sequence[str] | None = None,
    ) -> list[str] | None:
        """Return a deterministic list merging configured and required fields."""

        merged = merge_select_fields(select_fields, required_fields)
        if merged is None:
            return None
        return list(merged)

    def _dispatch_extract_mode(
        self,
        log: BoundLogger,
        *,
        event_name: str,
        batch_callback: Callable[[Sequence[str]], pd.DataFrame],
        full_callback: Callable[[], pd.DataFrame],
        id_column_name: str | None = None,
        legacy_id_resolver: (
            Callable[[BoundLogger], Sequence[str] | None] | None
        ) = None,
        legacy_source: str = "legacy",
    ) -> pd.DataFrame:
        """Dispatch extraction mode between batch and full strategies.

        This helper centralises reading identifiers from the CLI configuration,
        logging the selected execution mode, and invoking the provided
        callbacks for batch (ID-based) or full extraction.

        Parameters
        ----------
        log
            Logger bound to the caller's execution context.
        event_name
            Structured logging event emitted whenever the extraction mode is
            resolved.
        batch_callback
            Callback executed when identifiers are available. Receives the
            resolved list of identifiers.
        full_callback
            Callback executed when no identifiers are supplied via CLI or
            legacy hooks.
        id_column_name
            Optional override for the identifier column expected within the
            CLI input file. Defaults to the pipeline-derived value.
        legacy_id_resolver
            Optional callable that can provide identifiers from legacy inputs
            (for example deprecated keyword arguments). The callable receives
            the same logger so it can emit warnings prior to returning
            identifiers.
        legacy_source
            Source label used in structured logging when identifiers originate
            from the legacy resolver.
        """

        column_name = id_column_name or self._get_id_column_name()

        if self.config.cli.input_file:
            ids = self._read_input_ids(
                id_column_name=column_name,
                limit=self.config.cli.limit,
                sample=self.config.cli.sample,
            )
            if ids:
                log.info(
                    event_name,
                    mode="batch",
                    source="cli_input",
                    ids_count=len(ids),
                )
                return batch_callback(ids)

        if legacy_id_resolver is not None:
            legacy_ids = legacy_id_resolver(log)
            if legacy_ids:
                if isinstance(legacy_ids, (str, bytes)):
                    normalized_ids = [str(legacy_ids)]
                else:
                    normalized_ids = [str(item) for item in legacy_ids]
                if normalized_ids:
                    log.info(
                        event_name,
                        mode="batch",
                        source=legacy_source,
                        ids_count=len(normalized_ids),
                    )
                    return batch_callback(normalized_ids)

        log.info(event_name, mode="full")
        return full_callback()

    def run_extract_all(
        self: PipelineT, descriptor: ChemblExtractionDescriptor[PipelineT]
    ) -> pd.DataFrame:
        """Execute a descriptor-driven extraction loop with uniform metadata."""

        log = UnifiedLogger.get(__name__).bind(
            component=f"{self.pipeline_code}.extract"
        )
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config(descriptor.source_name)
        source_config = descriptor.source_config_factory(source_raw)

        context = descriptor.build_context(self, source_config, log)
        context.source_config = source_config

        resolved_release, release_metadata = self.ensure_chembl_release(
            context, log
        )

        limit = self.config.cli.limit

        configured_select: Sequence[str] | None = context.select_fields
        if (
            configured_select is None
            and descriptor.default_select_fields is not None
        ):
            configured_select = descriptor.default_select_fields
        merged_select = merge_select_fields(
            configured_select, descriptor.must_have_fields
        )
        select_fields_list = list(merged_select) if merged_select else None
        context.select_fields = select_fields_list

        batch_size_candidate: int | None = getattr(
            source_config, "batch_size", None
        )
        if batch_size_candidate is None:
            batch_size_candidate = getattr(source_config, "page_size", None)
        if batch_size_candidate is None:
            batch_size_candidate = self._resolve_batch_size(source_raw)

        hard_cap = descriptor.hard_page_size_cap
        if hard_cap is None and batch_size_candidate is not None:
            hard_cap = max(int(batch_size_candidate), 1)

        page_size = context.page_size
        if page_size is None:
            base_size = (
                batch_size_candidate
                if batch_size_candidate is not None
                else 25
            )
            cap = hard_cap if hard_cap is not None else base_size
            page_size = self._resolve_page_size(base_size, limit, hard_cap=cap)
        context.page_size = page_size

        normalised_parameters = self._normalize_parameters(
            source_config.parameters
        )

        _filters_payload, compact_filters = build_filters_payload(
            limit=limit,
            page_size=page_size,
            select_fields=select_fields_list,
            extra_filters=context.extra_filters,
            parameters=normalised_parameters,
            mode="all",
        )

        metadata_kwargs = self.publish_release_metadata(
            dict(context.metadata),
            release=resolved_release,
            metadata=release_metadata,
        )
        self.record_extract_metadata(
            filters=compact_filters,
            requested_at_utc=datetime.now(timezone.utc),
            **metadata_kwargs,
        )

        if self.config.cli.dry_run:
            if descriptor.dry_run_handler is not None:
                return descriptor.dry_run_handler(
                    self, context, log, stage_start
                )

            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            if descriptor.empty_frame_factory is not None:
                dataframe = descriptor.empty_frame_factory(self, context)
            else:
                dataframe = pd.DataFrame()

            dry_run_summary = self.publish_release_metadata(
                {
                    "rows": int(dataframe.shape[0]),
                    "duration_ms": duration_ms,
                    "dry_run": True,
                },
                release=resolved_release,
                metadata=release_metadata,
            )
            if descriptor.summary_extra is not None:
                dry_run_summary.update(
                    descriptor.summary_extra(self, dataframe, context)
                )
            if context.stats:
                dry_run_summary.update(context.stats)
            log.info(descriptor.summary_event, **dry_run_summary)
            return dataframe

        iterator_kwargs = dict(context.iterate_all_kwargs)
        records: list[dict[str, Any]] = []

        for payload in context.iterator.iterate_all(
            limit=limit,
            page_size=page_size,
            select_fields=select_fields_list,
            **iterator_kwargs,
        ):
            if descriptor.record_transform is not None:
                record_mapping = descriptor.record_transform(
                    self, payload, context
                )
            else:
                record_mapping = self._coerce_mapping(payload)
            records.append(dict(record_mapping))

        if records:
            dataframe = pd.DataFrame.from_records(records)
        elif descriptor.empty_frame_factory is not None:
            dataframe = descriptor.empty_frame_factory(self, context)
        else:
            dataframe = pd.DataFrame(
                {descriptor.id_column: pd.Series(dtype="object")}
            )

        if descriptor.sort_by and not dataframe.empty:
            if isinstance(descriptor.sort_by, Sequence) and not isinstance(
                descriptor.sort_by, (str, bytes)
            ):
                sort_columns = list(descriptor.sort_by)
            else:
                sort_columns = [str(descriptor.sort_by)]
            dataframe = dataframe.sort_values(sort_columns).reset_index(
                drop=True
            )

        for processor in descriptor.post_processors:
            dataframe = processor(self, dataframe, context, log)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0

        summary_payload = self.publish_release_metadata(
            {
                "rows": int(dataframe.shape[0]),
                "duration_ms": duration_ms,
            },
            release=resolved_release,
            metadata=release_metadata,
        )
        if context.stats:
            summary_payload.update(context.stats)
        if descriptor.summary_extra is not None:
            summary_payload.update(
                descriptor.summary_extra(self, dataframe, context)
            )

        log.info(descriptor.summary_event, **summary_payload)
        return dataframe

    def run_descriptor_extraction(
        self: PipelineT,
        descriptor: ChemblExtractionDescriptor[PipelineT],
        ids: Sequence[str] | None,
        *,
        summary_event: str,
        source_config: Any | None = None,
        dry_run_event: str | None = None,
        dry_run_handler: DryRunHandler | None = None,
        fetcher: FetcherCallable | None = None,
        fetcher_factory: FetcherFactory | None = None,
        finalize: FinalizeCallable | None = None,
        finalize_factory: FinalizeFactory | None = None,
        finalize_context: FinalizeContextCallable | None = None,
        finalize_context_factory: FinalizeContextFactory | None = None,
        summary_extra: Mapping[str, Any] | None = None,
        summary_extra_factory: SummaryFactory | None = None,
        metadata_filters: Mapping[str, Any] | None = None,
        chembl_release_override: str | None = None,
        fetch_mode: Literal["default", "delegated"] = "default",
        stats_attribute: str | None = None,
        id_normalizer: Callable[[Any], tuple[str | None, Any]] | None = None,
        empty_frame_factory: Callable[[], pd.DataFrame] | None = None,
        **batch_kwargs: Any,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        """Execute descriptor-driven ID extraction with shared orchestration.

        Parameters
        ----------
        descriptor
            Экземпляр :class:`ChemblExtractionDescriptor`, описывающий сущность,
            фабрики контекста и вспомогательные функции финализации.
        ids
            Набор идентификаторов для выборки. При ``None`` поведение определяется
            дескриптором (например, полная выгрузка).
        summary_event
            Имя события структурированного логирования для финального summary.
        source_config
            Предварительно типизированный конфиг источника; если не указан, он
            будет построен из ``descriptor.source_name``.
        dry_run_event
            Имя события, публикуемого при dry-run.
        dry_run_handler
            Пользовательский обработчик dry-run (например, генерация фиктивных
            данных). Если не задан, используется фабрика пустого DataFrame.
        fetcher / fetcher_factory
            Итератор по данным или фабрика, получающая контекст дескриптора.
        finalize / finalize_factory / finalize_context(...)
            Хуки для постобработки батчей и формирования агрегированной
            статистики.
        summary_extra / summary_extra_factory
            Дополнительные поля, включаемые в payload события summary.
        metadata_filters
            Фильтры, передаваемые в фабрику контекста/клиент. Используются для
            ограничения полей ответа.
        chembl_release_override
            Позволяет переопределить релиз из конфигурации/handshake.
        fetch_mode
            Управляет режимом перебора (``"default"`` или ``"delegated"``).
        stats_attribute
            Имя атрибута, куда будет записан :class:`BatchExtractionStats`.
        id_normalizer
            Колбек для приведения идентификаторов перед вызовом fetcher.
        empty_frame_factory
            Колбек, который возвращает предзаполненный пустой DataFrame для
            dry-run и случаев отсутствия данных.
        batch_kwargs
            Остальные параметры пробрасываются в :meth:`run_batched_extraction`.

        Returns
        -------
        tuple[pd.DataFrame, BatchExtractionStats]
            Кортеж из результирующего DataFrame и агрегированной статистики,
            включающей количество батчей, вызовов API и т.д.
        """

        if not summary_event:
            msg = "summary_event must be provided"
            raise ValueError(msg)

        canonical_ids = tuple(ids or ())
        stage_start = time.perf_counter()
        rows_hint = len(canonical_ids)
        stage_logger = getattr(self, "stage_logger", None)
        logger_cm: AbstractContextManager[BoundLogger]
        if callable(stage_logger):
            logger_cm = cast(
                AbstractContextManager[BoundLogger],
                stage_logger("extract", rows=rows_hint),  # pylint: disable=not-callable
            )
        else:
            fallback_log = UnifiedLogger.get(__name__).bind(
                component=f"{self.pipeline_code}.extract"
            )
            logger_cm = nullcontext(fallback_log)

        with logger_cm as log:
            if source_config is None:
                source_raw = self._resolve_source_config(
                    descriptor.source_name
                )
                typed_source_config = descriptor.source_config_factory(
                    source_raw
                )
            else:
                typed_source_config = source_config

            context = descriptor.build_context(self, typed_source_config, log)
            context.source_config = typed_source_config

            descriptor_empty_factory = descriptor.empty_frame_factory
            effective_empty_factory = empty_frame_factory
            if (
                effective_empty_factory is None
                and descriptor_empty_factory is not None
            ):

                def _descriptor_empty_factory() -> pd.DataFrame:
                    return descriptor_empty_factory(self, context)

                effective_empty_factory = _descriptor_empty_factory

            effective_fetcher = fetcher
            if effective_fetcher is None and fetcher_factory is not None:
                effective_fetcher = fetcher_factory(context, log)
            if effective_fetcher is None:
                entity_client = context.iterator
                iterate_candidate = getattr(
                    entity_client, "iterate_by_ids", None
                )
                if not callable(iterate_candidate):
                    msg = "Descriptor context does not expose iterate_by_ids()"
                    raise RuntimeError(msg)

                def _default_fetcher(
                    batch_ids: Sequence[str],
                    batch_context: BatchExtractionContext,
                ) -> Iterable[Mapping[str, Any]]:
                    iterator: Any = iterate_candidate(
                        batch_ids,
                        select_fields=batch_context.select_fields or None,
                    )
                    for item in iterator:
                        yield dict(item)

                effective_fetcher = _default_fetcher

            effective_finalize = finalize
            if effective_finalize is None and finalize_factory is not None:
                effective_finalize = finalize_factory(context, log)

            effective_finalize_context = finalize_context
            if (
                effective_finalize_context is None
                and finalize_context_factory is not None
            ):
                effective_finalize_context = finalize_context_factory(
                    context, log
                )

            resolved_release, release_metadata = self.ensure_chembl_release(
                context, log
            )

            if chembl_release_override is not None:
                resolved_release = chembl_release_override

            if (
                resolved_release is not None
                and self.chembl_release != resolved_release
            ):
                self._set_chembl_release(resolved_release)

            if self.config.cli.dry_run:
                stats = BatchExtractionStats(requested=len(canonical_ids))
                if dry_run_handler is not None:
                    dataframe = dry_run_handler(self, context, log)
                elif effective_empty_factory is not None:
                    dataframe = effective_empty_factory()
                else:
                    dataframe = pd.DataFrame()
                dry_run_payload = self.publish_release_metadata(
                    {
                        "dry_run": True,
                        "requested": len(canonical_ids),
                        "rows": int(dataframe.shape[0]),
                    },
                    release=resolved_release,
                    metadata=release_metadata,
                )
                if summary_extra:
                    dry_run_payload.update(summary_extra)
                if dry_run_event:
                    log.info(dry_run_event, **dry_run_payload)
                return dataframe, stats

            batch_args = dict(batch_kwargs)
            if (
                "id_column" not in batch_args
                or batch_args["id_column"] is None
            ):
                batch_args["id_column"] = descriptor.id_column
            if (
                batch_args.get("select_fields") is None
                and context.select_fields is not None
            ):
                batch_args["select_fields"] = context.select_fields
            if metadata_filters is not None:
                batch_args["metadata_filters"] = metadata_filters
            if effective_empty_factory is not None:
                batch_args["empty_frame_factory"] = effective_empty_factory
            if effective_finalize is not None:
                batch_args["finalize"] = effective_finalize
            if effective_finalize_context is not None:
                batch_args["finalize_context"] = effective_finalize_context
            if stats_attribute is not None:
                batch_args["stats_attribute"] = stats_attribute
            if id_normalizer is not None:
                batch_args["id_normalizer"] = id_normalizer
            if resolved_release is not None:
                batch_args = self.publish_release_metadata(
                    batch_args,
                    release=resolved_release,
                    include_metadata=False,
                )
            batch_args["fetcher"] = effective_fetcher

            dataframe, stats = self.run_batched_extraction(
                canonical_ids,
                fetch_mode=fetch_mode,
                **batch_args,
            )

            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            summary_payload = self.publish_release_metadata(
                {
                    "rows": int(dataframe.shape[0]),
                    "requested": len(canonical_ids),
                    "duration_ms": duration_ms,
                },
                release=resolved_release,
                metadata=release_metadata,
            )
            if stats.batches is not None:
                summary_payload["batches"] = stats.batches
            if stats.api_calls is not None:
                summary_payload["api_calls"] = stats.api_calls
            if stats.cache_hits is not None:
                summary_payload["cache_hits"] = stats.cache_hits
            if summary_extra:
                summary_payload.update(summary_extra)
            if summary_extra_factory is not None:
                summary_payload.update(summary_extra_factory(context, stats))

            log.info(summary_event, **summary_payload)
            return dataframe, stats

    # ------------------------------------------------------------------
    # API client management
    # ------------------------------------------------------------------

    def prepare_chembl_client(
        self,
        source_name: str = "chembl",
        *,
        base_url: str | None = None,
        client_name: str | None = None,
    ) -> tuple[UnifiedAPIClient, str]:
        """Prepare and register a ChEMBL API client.

        Parameters
        ----------
        source_name
            Name of the source configuration (default: "chembl").
        base_url
            Optional base URL override. If not provided, resolved from config.
        client_name
            Optional client registration name. If not provided, uses default.

        Returns
        -------
        tuple[UnifiedAPIClient, str]
            The prepared API client and resolved base URL.
        """
        source_config = self._resolve_source_config(source_name)
        options = {"base_url": base_url} if base_url else None
        client, resolved_base_url, _ = (
            self._chembl_entity_factory.build_http_client(
                source_name=source_name,
                source_config=source_config,
                options=options,
            )
        )
        if client_name:
            self.register_client(client_name, client)
        return client, resolved_base_url

    def build_chembl_entity_bundle(
        self,
        entity_name: str,
        *,
        source_name: str = "chembl",
        source_config: SourceConfig[Any] | None = None,
        options: Mapping[str, Any] | None = None,
        chembl_client_kwargs: Mapping[str, Any] | None = None,
        fresh_http_client: bool = False,
    ) -> ChemblClientBundle:
        """Создать сущностный клиент ChEMBL с общими параметрами пайплайна."""

        effective_source = source_config or self._resolve_source_config(
            source_name
        )
        kwargs = self._default_chembl_client_kwargs()
        if chembl_client_kwargs:
            kwargs.update(dict(chembl_client_kwargs))
        return self._chembl_entity_factory.build(
            entity_name,
            source_name=source_name,
            source_config=effective_source,
            options=options,
            chembl_client_kwargs=kwargs,
            fresh_http_client=fresh_http_client,
        )

    def _default_chembl_client_kwargs(self) -> dict[str, Any]:
        """Вернуть контекст по умолчанию для инициализации ChemblClient."""

        return {
            "load_meta_store": self.load_meta_store,
            "job_id": self.run_id,
            "operator": self.pipeline_code,
        }

    # ------------------------------------------------------------------
    # Release metadata helpers
    # ------------------------------------------------------------------

    def chembl_release_metadata(self) -> dict[str, Any]:
        """Return a snapshot of cached release metadata."""

        return dict(self._chembl_release_metadata)

    def update_chembl_release_metadata(self, **metadata: Any) -> None:
        """Merge additional metadata into the cached release payload."""

        if not metadata:
            return

        for key, value in metadata.items():
            if value is None:
                self._chembl_release_metadata.pop(key, None)
                continue
            self._chembl_release_metadata[key] = value

    def publish_release_metadata(
        self,
        payload: Mapping[str, Any] | None = None,
        *,
        release: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        include_metadata: bool = True,
    ) -> dict[str, Any]:
        """Attach release/version info to a structured logging payload."""

        merged: dict[str, Any] = dict(payload or {})
        resolved_release = release or self.chembl_release
        if resolved_release is not None:
            merged["chembl_release"] = resolved_release

        if include_metadata:
            metadata_payload = self.chembl_release_metadata()
            if metadata:
                metadata_payload.update(
                    {k: v for k, v in metadata.items() if v is not None}
                )
            if metadata_payload:
                merged.update(metadata_payload)

        return merged

    def ensure_chembl_release(
        self,
        context: ChemblExtractionContext,
        log: BoundLogger,
    ) -> tuple[str | None, dict[str, Any]]:
        """Ensure the ChEMBL release is resolved and cached for the given context."""

        resolved_release = context.chembl_release or self.chembl_release
        resolver = context.release_resolver
        chembl_client = context.chembl_client
        entity_client = context.iterator

        if resolved_release is None and callable(resolver):
            try:
                release_candidate = resolver(
                    self, chembl_client, log, entity_client
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    LogEvents.CHEMBL_DESCRIPTOR_STATUS_FAILED, error=str(exc)
                )
            else:
                if release_candidate:
                    resolved_release = release_candidate

        if resolved_release is None and chembl_client is not None:
            release_candidate, metadata = self.resolve_chembl_release(
                chembl_client,
                log,
                entity_client,
            )
            if release_candidate:
                resolved_release = release_candidate
            if metadata:
                self.update_chembl_release_metadata(**metadata)

        if resolved_release is not None:
            if context.chembl_release != resolved_release:
                context.chembl_release = resolved_release
            if self.chembl_release != resolved_release:
                self._set_chembl_release(resolved_release)

        return resolved_release, self.chembl_release_metadata()

    # ------------------------------------------------------------------
    # ChEMBL release fetching
    # ------------------------------------------------------------------

    def fetch_chembl_release(
        self,
        client: UnifiedAPIClient | Any,  # pyright: ignore[reportAny]
        log: BoundLogger | None = None,
    ) -> str | None:
        """Fetch ChEMBL release version from status endpoint.

        Supports both UnifiedAPIClient (direct HTTP) and ChemblClient
        (wrapped with handshake) interfaces.

        Parameters
        ----------
        client
            API client (UnifiedAPIClient or ChemblClient).
        log
            Optional logger instance. If not provided, creates one.

        Returns
        -------
        str | None
            The ChEMBL release version, or None if unavailable.
        """
        if log is None:
            log = UnifiedLogger.get(__name__).bind(
                component=f"{self.pipeline_code}.extract"
            )

        release_value: str | None = None

        # Check if client is ChemblClient by checking for handshake method
        handshake_candidate = getattr(client, "handshake", None)
        if callable(handshake_candidate):
            handshake = cast(Callable[..., Any], handshake_candidate)
            request_timestamp = datetime.now(timezone.utc)
            try:
                status = handshake()
                if isinstance(status, Mapping):
                    status_mapping = cast(Mapping[str, Any], status)
                    candidate = status_mapping.get(
                        "chembl_db_version"
                    ) or status_mapping.get("chembl_release")
                    if isinstance(candidate, str):
                        release_value = candidate
                        log.info(
                            LogEvents.CHEMBL_DESCRIPTOR_STATUS,
                            chembl_release=release_value,
                        )
                    # Extract and set api_version if present
                    api_version_candidate = status_mapping.get("api_version")
                    if api_version_candidate is not None:
                        self._set_api_version(str(api_version_candidate))
            except Exception as exc:
                log.warning(
                    LogEvents.CHEMBL_DESCRIPTOR_STATUS_FAILED, error=str(exc)
                )
            finally:
                self.record_extract_metadata(
                    chembl_release=release_value,
                    requested_at_utc=request_timestamp,
                )
            return release_value

        # Use direct HTTP for UnifiedAPIClient
        get_candidate = getattr(client, "get", None)
        if callable(get_candidate):
            client_get = cast(Callable[..., Any], get_candidate)
            request_timestamp = datetime.now(timezone.utc)
            status_endpoint = _resolve_status_endpoint()
            try:
                response = client_get(status_endpoint)
                json_candidate = getattr(response, "json", None)
                if callable(json_candidate):
                    status_payload_raw = json_candidate()
                    status_payload = self._coerce_mapping(status_payload_raw)
                    release_value = self._extract_chembl_release(
                        status_payload
                    )
                    log.info(
                        LogEvents.CHEMBL_DESCRIPTOR_STATUS,
                        chembl_release=release_value,
                    )
                    # Extract and set api_version if present
                    api_version_candidate = status_payload.get("api_version")
                    if api_version_candidate is not None:
                        self._set_api_version(str(api_version_candidate))
            except Exception as exc:
                log.warning(
                    LogEvents.CHEMBL_DESCRIPTOR_STATUS_FAILED, error=str(exc)
                )
            finally:
                self.record_extract_metadata(
                    chembl_release=release_value,
                    requested_at_utc=request_timestamp,
                )
            return release_value
        self.record_extract_metadata(
            requested_at_utc=datetime.now(timezone.utc)
        )
        return None

    def resolve_chembl_release(
        self,
        chembl_client: UnifiedAPIClient | Any,  # pyright: ignore[reportAny]
        log: BoundLogger,
        entity_client: Any | None = None,  # noqa: ARG002
    ) -> tuple[str | None, dict[str, Any]]:
        """Resolve Chembl release number and metadata for the current run.

        Subclasses may override this hook to provide additional metadata derived
        from entity clients (например, `api_version` у test item пайплайна).

        Returns
        -------
        tuple[str | None, dict[str, Any]]
            Кортеж из версии релиза (если найдена) и дополнительных полей,
            которые попадут в финальные логи/manifest.
        """
        _ = "Resolve Chembl release and optional metadata for ID extractions."
        _ = entity_client

        metadata: dict[str, Any] = self.chembl_release_metadata()

        chembl_db_version = getattr(self, "chembl_db_version", None)
        if chembl_db_version:
            metadata["chembl_db_version"] = chembl_db_version

        api_version = self.api_version
        if api_version:
            metadata["api_version"] = api_version

        cached_release = self.chembl_release
        if cached_release:
            return cached_release, metadata

        try:
            release_value = self.fetch_chembl_release(chembl_client, log)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                LogEvents.CHEMBL_DESCRIPTOR_STATUS_FAILED, error=str(exc)
            )
            return None, {}

        if metadata:
            self.update_chembl_release_metadata(**metadata)
            metadata = self.chembl_release_metadata()

        if release_value is not None and self.chembl_release != release_value:
            self._set_chembl_release(release_value)

        return release_value, metadata

    def _fetch_chembl_release(
        self,
        client: UnifiedAPIClient | Any,  # pyright: ignore[reportAny]
        log: BoundLogger | None = None,
    ) -> str | None:
        """Backward compatible wrapper for tests expecting private method."""

        return self.fetch_chembl_release(client, log)

    @staticmethod
    def _extract_chembl_release(
        payload: Mapping[str, Any] | None
    ) -> str | None:
        """Extract ChEMBL release version from status payload.

        Parameters
        ----------
        payload
            Status response payload mapping.

        Returns
        -------
        str | None
            The release version string, or None if not found.
        """
        if not payload:
            return None

        for key in (
            "chembl_release",
            "chembl_db_version",
            "release",
            "version",
        ):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
            if value is not None:
                return str(value)
        return None

    # ------------------------------------------------------------------
    # Response processing utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_mapping(payload: Any) -> dict[str, Any]:
        """Coerce payload to dictionary mapping.

        Parameters
        ----------
        payload
            Response payload (may be dict, Mapping, or other).

        Returns
        -------
        dict[str, Any]
            Dictionary representation of the payload.
        """
        if isinstance(payload, Mapping):
            mapping = cast(Mapping[object, Any], payload)
            return ChemblPipelineBase._stringify_mapping(mapping)
        return {}

    @staticmethod
    def _extract_page_items(
        payload: Mapping[str, Any],
        items_keys: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Extract items from paginated response payload.

        Parameters
        ----------
        payload
            Paginated response payload mapping.
        items_keys
            Optional sequence of keys to check for items. If not provided,
            uses common defaults: ("data", "items", "results").

        Returns
        -------
        list[dict[str, Any]]
            List of extracted item dictionaries.
        """
        if items_keys is None:
            items_keys = ("data", "items", "results")

        for key in items_keys:
            value = payload.get(key)
            if isinstance(value, Sequence) and not isinstance(
                value, (str, bytes, bytearray)
            ):
                candidates: list[dict[str, Any]] = []
                sequence_items = cast(Sequence[object], value)
                for item in sequence_items:
                    if isinstance(item, Mapping):
                        mapping = cast(Mapping[object, Any], item)
                        candidates.append(
                            ChemblPipelineBase._stringify_mapping(mapping)
                        )
                if candidates:
                    return candidates

        # Fallback: iterate all keys except page_meta
        for key, value in payload.items():
            if key == "page_meta":
                continue
            if isinstance(value, Sequence) and not isinstance(
                value, (str, bytes, bytearray)
            ):
                candidates = []
                sequence_items = cast(Sequence[object], value)
                for item in sequence_items:
                    if isinstance(item, Mapping):
                        mapping = cast(Mapping[object, Any], item)
                        candidates.append(
                            ChemblPipelineBase._stringify_mapping(mapping)
                        )
                if candidates:
                    return candidates
        return []

    @staticmethod
    def _next_link(payload: Mapping[str, Any], base_url: str) -> str | None:
        """Extract next page link from paginated response.

        Parameters
        ----------
        payload
            Paginated response payload mapping.
        base_url
            Base URL for the API (used to normalize full URLs to relative paths).

        Returns
        -------
        str | None
            Relative path for the next page, or None if no next page.
        """
        page_meta = payload.get("page_meta")
        if not isinstance(page_meta, Mapping):
            return None

        page_meta_mapping = cast(Mapping[str, Any], page_meta)
        next_link_raw = page_meta_mapping.get("next")
        if not isinstance(next_link_raw, str):
            return None

        next_link = next_link_raw.strip()
        if not next_link:
            return None

        # If next_link is a full URL, extract only the relative path
        if next_link.startswith("http://") or next_link.startswith("https://"):
            parsed = urlparse(next_link)
            base_parsed = urlparse(base_url)

            # Normalize paths: remove trailing slashes for comparison
            path = parsed.path.rstrip("/")
            base_path = base_parsed.path.rstrip("/")

            # If paths match, return just the path with query
            if path == base_path or path.startswith(f"{base_path}/"):
                relative_path = (
                    path[len(base_path) :]
                    if path.startswith(base_path)
                    else path
                )
                if parsed.query:
                    return f"{relative_path}?{parsed.query}"
                return relative_path

            # If base paths don't match, return full URL path + query
            if parsed.query:
                return f"{parsed.path}?{parsed.query}"
            return parsed.path

        # Already a relative path
        return next_link

    # ------------------------------------------------------------------
    # Batch extraction utilities
    # ------------------------------------------------------------------

    def run_batched_extraction(
        self,
        ids: Sequence[Any],
        *,
        id_column: str,
        fetcher: Callable[[Sequence[str], BatchExtractionContext], Any],
        select_fields: Sequence[str] | None = None,
        required_fields: Sequence[str] | None = None,
        limit: int | None = None,
        batch_size: int | None = None,
        chunk_size: int | None = None,
        max_batch_size: int | None = 25,
        metadata_filters: Mapping[str, Any] | None = None,
        chembl_release: str | None = None,
        id_normalizer: Callable[[Any], tuple[str | None, Any]] | None = None,
        sort_key: Callable[[tuple[str, Any]], Any] | None = None,
        transform_item: (
            Callable[
                [Mapping[str, Any], BatchExtractionContext], Mapping[str, Any]
            ]
            | None
        ) = None,
        finalize: (
            Callable[[pd.DataFrame, BatchExtractionContext], pd.DataFrame]
            | None
        ) = None,
        finalize_context: (
            Callable[[BatchExtractionContext], None] | None
        ) = None,
        empty_frame_factory: Callable[[], pd.DataFrame] | None = None,
        stats_attribute: str | None = None,
        fetch_mode: Literal["default", "delegated"] = "default",
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        """Execute a reusable batch extraction routine with deterministic semantics."""

        log = UnifiedLogger.get(__name__).bind(
            component=f"{self.pipeline_code}.extract"
        )
        method_start = time.perf_counter()

        merged_select_fields = self._merge_select_fields(
            select_fields, required_fields
        )
        select_fields_tuple: tuple[str, ...] = tuple(
            merged_select_fields or ()
        )

        strategy_factory = self.get_descriptor_strategy_factory()
        plan = strategy_factory.build_plan(
            pipeline=cast(PipelineBase, self),
            ids=ids,
            id_column=id_column,
            select_fields=select_fields_tuple,
            fetcher=fetcher,
            fetch_mode=fetch_mode,
            limit=limit,
            batch_size=batch_size,
            chunk_size=chunk_size,
            max_batch_size=max_batch_size,
            id_normalizer=id_normalizer,
            sort_key=sort_key,
            transform_item=transform_item,
            finalize=finalize,
            finalize_context=finalize_context,
            empty_frame_factory=empty_frame_factory,
            stats_attribute=stats_attribute,
            log=log,
            started_at=method_start,
        )

        extra_filter_payload: dict[str, Any] = {
            "requested_ids": list(plan.context.ids),
            "batch_size": plan.context.batch_size,
        }
        if metadata_filters:
            extra_filter_payload.update(dict(metadata_filters))
        _filters_payload, compact_filters = build_filters_payload(
            mode="ids",
            limit=plan.context.limit,
            page_size=plan.context.batch_size,
            select_fields=merged_select_fields,
            extra_filters=extra_filter_payload,
        )

        release_payload = self.publish_release_metadata(
            {},
            release=chembl_release or self.chembl_release,
        )
        self.record_extract_metadata(
            filters=compact_filters,
            requested_at_utc=datetime.now(timezone.utc),
            **release_payload,
        )

        result = plan.execute()  # pyright: ignore[reportGeneralTypeIssues]
        return cast(tuple[pd.DataFrame, BatchExtractionStats], result)

    def extract_ids_paginated(
        self,
        ids: Sequence[str],
        endpoint: str,
        id_column: str,
        id_param_name: str,
        client: UnifiedAPIClient,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        select_fields: Sequence[str] | None = None,
        items_keys: Sequence[str] | None = None,
        process_item: Any | None = None,
    ) -> pd.DataFrame:
        """Extract records by batching ID values with pagination support.

        Parameters
        ----------
        ids
            Sequence of ID values to extract.
        endpoint
            API endpoint path (e.g., "/activity.json", "/document.json").
        id_column
            Name of the ID column in the resulting DataFrame.
        id_param_name
            API parameter name for ID filtering (e.g., "activity_id__in", "document_chembl_id__in").
        client
            Unified API client instance.
        batch_size
            Optional batch size override. If not provided, resolved from config.
        limit
            Optional limit on total number of records to extract.
        select_fields
            Optional list of fields to select from the API.
        items_keys
            Optional keys to check for items in response (passed to _extract_page_items).
        process_item
            Optional callable to process each item before adding to results.

        Returns
        -------
        pd.DataFrame
            DataFrame containing extracted records, sorted by ID column.
        """
        log = UnifiedLogger.get(__name__).bind(
            component=f"{self.pipeline_code}.extract"
        )
        method_start = time.perf_counter()

        if batch_size is None:
            source_config = self._resolve_source_config("chembl")
            batch_size = self._resolve_batch_size(source_config)

        # Ensure batch_size does not exceed ChEMBL API limit
        batch_size = min(batch_size, 25)
        batch_size = max(batch_size, 1)

        # Extract unique IDs, filter out NaN, sort for determinism
        unique_ids = sorted(
            {str(id_val) for id_val in ids if id_val and str(id_val).strip()}
        )

        if not unique_ids:
            log.debug(LogEvents.EXTRACT_IDS_PAGINATED_NO_VALID_IDS)
            return pd.DataFrame({id_column: pd.Series(dtype="string")})

        # Process in batches
        all_records: list[dict[str, Any]] = []
        batches = 0
        api_calls = 0

        for i in range(0, len(unique_ids), batch_size):
            batch_ids = unique_ids[i : i + batch_size]
            batches += 1

            params: dict[str, Any] = {
                id_param_name: ",".join(batch_ids),
                "limit": batch_size,
            }
            if select_fields:
                params["only"] = ",".join(select_fields)

            try:
                response = client.get(endpoint, params=params)
                api_calls += 1
                payload = self._coerce_mapping(response.json())
                page_items = self._extract_page_items(
                    payload, items_keys=items_keys
                )

                for item in page_items:
                    item_dict = dict(item)
                    processed_item = (
                        process_item(item_dict) if process_item else item_dict
                    )
                    all_records.append(processed_item)

                if limit is not None and len(all_records) >= limit:
                    all_records = all_records[:limit]
                    break

            except Exception as exc:
                log.warning(
                    LogEvents.EXTRACT_IDS_PAGINATED_BATCH_ERROR,
                    batch_ids=batch_ids,
                    error=str(exc),
                    exc_info=True,
                )

        dataframe = pd.DataFrame.from_records(
            all_records
        )  # pyright: ignore[reportUnknownMemberType]
        if dataframe.empty:
            dataframe = pd.DataFrame({id_column: pd.Series(dtype="string")})
        elif id_column in dataframe.columns:
            dataframe = dataframe.sort_values(id_column).reset_index(drop=True)

        duration_ms = (time.perf_counter() - method_start) * 1000.0
        log.info(
            LogEvents.EXTRACT_IDS_PAGINATED_SUMMARY,
            rows=int(dataframe.shape[0]),
            requested=len(unique_ids),
            batches=batches,
            api_calls=api_calls,
            duration_ms=duration_ms,
        )

        return dataframe
