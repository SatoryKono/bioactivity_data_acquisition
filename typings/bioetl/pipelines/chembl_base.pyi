from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.config.models.source import SourceConfig


class ChemblPipelineBase: ...


class ChemblExtractionContext:
    source_config: Any
    iterator: Any
    chembl_client: Any | None
    select_fields: Sequence[str] | None
    page_size: int | None
    chembl_release: str | None
    metadata: dict[str, Any]
    extra_filters: dict[str, Any]
    iterate_all_kwargs: dict[str, Any]
    stats: dict[str, Any]

    def __init__(
        self,
        source_config: Any,
        iterator: Any,
        chembl_client: Any | None = ...,
        select_fields: Sequence[str] | None = ...,
        page_size: int | None = ...,
        chembl_release: str | None = ...,
        metadata: dict[str, Any] | None = ...,
        extra_filters: dict[str, Any] | None = ...,
        iterate_all_kwargs: dict[str, Any] | None = ...,
        stats: dict[str, Any] | None = ...,
    ) -> None: ...


class ChemblExtractionDescriptor:
    name: str
    source_name: str
    source_config_factory: Callable[[SourceConfig], Any]
    build_context: Callable[[ChemblPipelineBase, Any, BoundLogger], ChemblExtractionContext]
    id_column: str
    summary_event: str
    must_have_fields: Sequence[str]
    default_select_fields: Sequence[str] | None
    record_transform: Callable[
        [ChemblPipelineBase, Mapping[str, Any], ChemblExtractionContext],
        Mapping[str, Any],
    ] | None
    post_processors: Sequence[
        Callable[[ChemblPipelineBase, pd.DataFrame, ChemblExtractionContext, BoundLogger], pd.DataFrame]
    ]
    sort_by: Sequence[str] | str | None
    empty_frame_factory: Callable[[ChemblPipelineBase, ChemblExtractionContext], pd.DataFrame] | None
    dry_run_handler: Callable[
        [ChemblPipelineBase, ChemblExtractionContext, BoundLogger, float],
        pd.DataFrame,
    ] | None
    hard_page_size_cap: int | None
    summary_extra: Callable[
        [ChemblPipelineBase, pd.DataFrame, ChemblExtractionContext],
        Mapping[str, Any],
    ] | None

    def __init__(
        self,
        *,
        name: str,
        source_name: str,
        source_config_factory: Callable[[SourceConfig], Any],
        build_context: Callable[[ChemblPipelineBase, Any, BoundLogger], ChemblExtractionContext],
        id_column: str,
        summary_event: str,
        must_have_fields: Sequence[str] = ...,
        default_select_fields: Sequence[str] | None = ...,
        record_transform: Callable[
            [ChemblPipelineBase, Mapping[str, Any], ChemblExtractionContext],
            Mapping[str, Any],
        ] | None = ...,
        post_processors: Sequence[
            Callable[[ChemblPipelineBase, pd.DataFrame, ChemblExtractionContext, BoundLogger], pd.DataFrame]
        ] = ...,
        sort_by: Sequence[str] | str | None = ...,
        empty_frame_factory: Callable[[ChemblPipelineBase, ChemblExtractionContext], pd.DataFrame] | None = ...,
        dry_run_handler: Callable[
            [ChemblPipelineBase, ChemblExtractionContext, BoundLogger, float],
            pd.DataFrame,
        ] | None = ...,
        hard_page_size_cap: int | None = ...,
        summary_extra: Callable[
            [ChemblPipelineBase, pd.DataFrame, ChemblExtractionContext],
            Mapping[str, Any],
        ] | None = ...,
    ) -> None: ...


class BatchExtractionStats:
    requested: int
    rows: int
    batches: int
    api_calls: int | None
    cache_hits: int | None
    duration_ms: float | None
    extra: dict[str, Any]

    def __init__(
        self,
        *,
        requested: int,
        rows: int = ...,
        batches: int = ...,
        api_calls: int | None = ...,
        cache_hits: int | None = ...,
        duration_ms: float | None = ...,
        extra: dict[str, Any] | None = ...,
    ) -> None: ...

    def as_dict(self) -> dict[str, Any]: ...
    def for_logging(self) -> dict[str, Any]: ...
    def set_extra(self, **kwargs: Any) -> None: ...


class BatchExtractionContext:
    ids: tuple[str, ...]
    id_column: str
    select_fields: tuple[str, ...]
    limit: int | None
    batch_size: int
    chunk_size: int
    stats: BatchExtractionStats
    log: BoundLogger
    metadata: dict[str, Any]
    extra: dict[str, Any]

    def __init__(
        self,
        *,
        ids: tuple[str, ...],
        id_column: str,
        select_fields: tuple[str, ...],
        limit: int | None,
        batch_size: int,
        chunk_size: int,
        stats: BatchExtractionStats,
        log: BoundLogger,
        metadata: dict[str, Any] | None = ...,
        extra: dict[str, Any] | None = ...,
    ) -> None: ...

    def increment_batches(self) -> None: ...
    def increment_api_calls(self, *, count: int = ...) -> None: ...
    def set_cache_hits(self, value: int | None) -> None: ...
    def set_extra(self, **kwargs: Any) -> None: ...
