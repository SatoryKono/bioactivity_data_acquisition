"""Typed contracts bridging configuration models и пайплайны.

Модуль выделен отдельно, чтобы `bioetl.config` и `bioetl.core.pipeline`
не импортировали друг друга напрямую. Протоколы описывают минимальный
набор атрибутов, ожидаемый `PipelineBase`.

>>> from bioetl.core.config_contracts import PipelineConfigProtocol
>>> hasattr(PipelineConfigProtocol, "__module__")
True
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, TypeVar, runtime_checkable

__all__ = [
    "PipelineCacheConfigProtocol",
    "PipelineCLIConfigProtocol",
    "PipelineDeterminismConfigProtocol",
    "PipelineDeterminismEnvironmentProtocol",
    "PipelineDeterminismHashingProtocol",
    "PipelineDeterminismSortingProtocol",
    "PipelineMaterializationConfigProtocol",
    "PipelineMetadataProtocol",
    "PipelinePathsConfigProtocol",
    "PipelineTransformConfigProtocol",
    "PipelineValidationConfigProtocol",
    "PipelineConfigProtocol",
    "SourceConfigProtocol",
    "SupportsModelCopy",
]


_SupportsModelCopyT = TypeVar("_SupportsModelCopyT", bound="SupportsModelCopy")


class SupportsModelCopy(Protocol):
    """Минимальный контракт, реализуемый pydantic-моделями с ``model_copy``."""

    def model_copy(
        self: _SupportsModelCopyT,
        *,
        update: Mapping[str, Any] | None = None,
    ) -> _SupportsModelCopyT:
        ...


@runtime_checkable
class PipelineMetadataProtocol(Protocol):
    """Минимальные метаданные о пайплайне."""

    name: str
    version: str


@runtime_checkable
class PipelineMaterializationConfigProtocol(Protocol):
    """Файловые корни, используемые пайплайнами."""

    root: str


@runtime_checkable
class PipelinePathsConfigProtocol(Protocol):
    """Глобальные пути, объявленные в конфигурации."""

    input_root: str
    output_root: str
    samples_root: str
    remote_output_root: str | None
    cache_root: str


@runtime_checkable
class PipelineCacheConfigProtocol(Protocol):
    """Конфигурация кэша, разделяемая пайплайнами."""

    enabled: bool
    directory: str
    ttl: int


@runtime_checkable
class PipelineCLIConfigProtocol(Protocol):
    """CLI-переопределения, прокинутые в пайплайны."""

    date_tag: str | None
    input_file: str | None
    limit: int | None
    fail_on_schema_drift: bool
    validate_columns: bool
    extended: bool
    dry_run: bool
    verbose: bool
    sample: int | None
    golden: str | None
    set_overrides: Mapping[str, Any] | None


@runtime_checkable
class PipelineTransformConfigProtocol(Protocol):
    """Настройки стадии transform."""

    arrays_to_header_rows: Sequence[str]
    enable_flatten: bool
    enable_serialization: bool
    arrays_simple_to_pipe: Sequence[str]
    arrays_objects_to_header_rows: Sequence[str]
    flatten_objects: Mapping[str, Sequence[str]]


@runtime_checkable
class PipelineDeterminismEnvironmentProtocol(Protocol):
    """Часовой пояс и локаль для детерминизма."""

    timezone: str


@runtime_checkable
class PipelineDeterminismHashingProtocol(Protocol):
    """Политика хеширования, требуемая детерминированными выгрузками."""

    business_key_fields: Sequence[str] | None
    row_hash_column: str


@runtime_checkable
class PipelineDeterminismSortingProtocol(Protocol):
    """Стратегия сортировки для детерминированных выгрузок."""

    by: list[str]
    ascending: list[bool]
    na_position: str


@runtime_checkable
class PipelineDeterminismConfigProtocol(SupportsModelCopy, Protocol):
    """Агрегированные настройки детерминизма."""

    environment: PipelineDeterminismEnvironmentProtocol
    hashing: PipelineDeterminismHashingProtocol
    sort: PipelineDeterminismSortingProtocol


@runtime_checkable
class PipelineValidationConfigProtocol(SupportsModelCopy, Protocol):
    """Ссылки на схемы и строгий режим валидации."""

    schema_in: str | None
    schema_out: str | None
    schema_in_version: str | None
    schema_out_version: str | None
    allow_schema_migration: bool
    max_schema_migration_hops: int | None
    strict: bool
    coerce: bool


@runtime_checkable
class SourceConfigProtocol(Protocol):
    """Минимальный интерфейс для переопределений источников."""

    parameters: Mapping[str, Any] | None


@runtime_checkable
class PipelineConfigProtocol(SupportsModelCopy, Protocol):
    """Структурное представление, общее для лоадера и пайплайнов."""

    pipeline: PipelineMetadataProtocol
    materialization: PipelineMaterializationConfigProtocol
    cli: PipelineCLIConfigProtocol
    determinism: PipelineDeterminismConfigProtocol
    validation: PipelineValidationConfigProtocol
    paths: PipelinePathsConfigProtocol
    cache: PipelineCacheConfigProtocol
    transform: PipelineTransformConfigProtocol
    sources: Mapping[str, SourceConfigProtocol]
    chembl: Mapping[str, Any] | None


