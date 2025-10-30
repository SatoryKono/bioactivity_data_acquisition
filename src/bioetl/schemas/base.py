"""Base Pandera schemas and shared helpers."""

from __future__ import annotations

from typing import Any, Protocol, TypedDict, cast

import pandas as pd

from bioetl.pandera_pandas import DataFrameModel, Field
from bioetl.pandera_typing import DataFrame, Series

# Shared column order for fallback metadata columns.  Exposed as a module level
# constant so downstream schemas can reference it without importing the mixin
# (which would re-export the Pandera ``Series`` annotations).
FALLBACK_METADATA_COLUMN_ORDER = [
    "fallback_reason",
    "fallback_error_type",
    "fallback_error_code",
    "fallback_error_message",
    "fallback_http_status",
    "fallback_retry_after_sec",
    "fallback_attempt",
    "fallback_timestamp",
]

class _SchemaConfigProtocol(Protocol):
    """Protocol describing the dynamic Pandera ``Config`` object."""

    _schema_cls: type[BaseSchema]
    column_order: Any


class _FieldSpec(TypedDict, total=False):
    nullable: bool
    description: str
    regex: str
    ge: float | int


class FallbackMetadataMixin:
    """Reusable Pandera column definitions for fallback metadata fields."""

    # Use a stable annotation that doesn't rely on typing.Any to avoid
    # evaluation issues in foreign modules during Pandera introspection.
    _FIELD_SPECS: dict[str, dict[str, object]] = {
        "fallback_reason": {
            "nullable": True,
            "description": "Reason why the fallback record was generated",
        },
        "fallback_error_type": {
            "nullable": True,
            "description": "Exception class that triggered the fallback",
        },
        "fallback_error_code": {
            "nullable": True,
            "description": "Normalized error code captured for the fallback",
        },
        "fallback_error_message": {
            "nullable": True,
            "description": "Human readable error message captured for the fallback",
        },
        "fallback_http_status": {
            "nullable": True,
            "ge": 0,
            "description": "HTTP status associated with the fallback (if any)",
        },
        "fallback_retry_after_sec": {
            "nullable": True,
            "ge": 0,
            "description": "Retry-After header (seconds) returned by the upstream API",
        },
        "fallback_attempt": {
            "nullable": True,
            "ge": 0,
            "description": "Attempt number when the fallback was emitted",
        },
        "fallback_timestamp": {
            "nullable": True,
            "description": "UTC timestamp when the fallback record was materialised",
        },
    }

    fallback_reason: Series[str]
    fallback_error_type: Series[str]
    fallback_error_code: Series[str]
    fallback_error_message: Series[str]
    fallback_http_status: Series[pd.Int64Dtype]
    fallback_retry_after_sec: Series[float]
    fallback_attempt: Series[pd.Int64Dtype]
    fallback_timestamp: Series[str]

    # Populate class attributes with Field definitions so that the mixin can be
    # used directly in schemas. Pandera replaces these attributes with string
    # sentinels during class creation, therefore the original ``FieldInfo``
    # instances are recreated as needed when propagating to subclasses.
    for _name, _spec in _FIELD_SPECS.items():
        locals()[_name] = Field(**_spec)
    del _name, _spec

    def __init_subclass__(cls, **kwargs: Any) -> None:  # pragma: no cover - executed on subclass creation
        """Ensure mixin annotations propagate to subclasses for Pandera."""

        super().__init_subclass__(**kwargs)

        # The hook also executes for the mixin itself. Only propagate the
        # annotations when a concrete schema inherits from the mixin.
        if cls is FallbackMetadataMixin:
            return

        mixin_annotations = getattr(FallbackMetadataMixin, "__annotations__", {})
        if not isinstance(mixin_annotations, dict) or not mixin_annotations:
            return

        target_annotations = dict(getattr(cls, "__annotations__", {}))

        for name, annotation in mixin_annotations.items():
            target_annotations.setdefault(name, annotation)
            if name not in cls.__dict__:
                field = FallbackMetadataMixin.__dict__.get(name)
                if field is not None:
                    setattr(cls, name, field)

        cls.__annotations__ = target_annotations


class _ColumnOrderAccessor:
    """Descriptor exposing schema ``column_order`` without Pandera side-effects."""

    def __init__(self, schema_cls: type[BaseSchema]) -> None:
        self._schema_cls = schema_cls

    def __get__(
        self,
        instance: Any,
        owner: type[BaseSchema],
    ) -> list[str]:  # noqa: D401 - standard descriptor signature
        schema_cls = getattr(owner, "_schema_cls", self._schema_cls)
        return schema_cls.get_column_order()


def expose_config_column_order(schema_cls: type[BaseSchema]) -> None:
    """Attach a lazy ``column_order`` accessor to ``schema_cls.Config``.

    Pandera treats arbitrary attributes defined on ``Config`` as potential
    dataframe-level checks during ``DataFrameModel`` registration.  Assigning a
    plain ``list`` therefore causes Pandera to look for a ``Check.column_order``
    method, resulting in ``AttributeError`` at runtime.

    This helper installs a descriptor that simply proxies to the schema's
    ``get_column_order`` method, avoiding the registration side-effects while
    preserving the existing ``Config.column_order`` contract used by
    downstream tooling (column validators, deterministic writers, etc.).
    """

    accessor = _ColumnOrderAccessor(schema_cls)
    config = cast(_SchemaConfigProtocol, schema_cls.Config)
    config._schema_cls = schema_cls
    config.column_order = accessor

    extras = getattr(schema_cls, "__extras__", None)
    if isinstance(extras, dict) and "column_order" in extras:
        # Pandera inspects ``__extras__`` to register dataframe-level checks.
        # Remove ``column_order`` so the descriptor isn't treated as a Check.
        updated = dict(extras)
        updated.pop("column_order", None)
        schema_cls.__extras__ = updated


class BaseSchema(DataFrameModel):
    """Базовый класс для Pandera схем.

    Содержит обязательные системные поля для всех пайплайнов:
    - index: детерминированный индекс строки
    - hash_row: SHA256 от канонической строки (для проверки целостности)
    - hash_business_key: SHA256 от бизнес-ключа (для дедупликации)
    - pipeline_version: версия пайплайна
    - run_id: идентификатор конкретного запуска пайплайна
    - source_system: источник данных
    - chembl_release: версия ChEMBL
    - extracted_at: метка времени извлечения (ISO8601)
    """

    hash_policy_version = "1.0.0"

    # Детерминизм и система трекинга
    index: Series[int] = Field(nullable=False, ge=0, description="Детерминированный индекс строки")
    hash_row: Series[str] = Field(
        nullable=False,
        regex=r'^[0-9a-f]{64}$',
        description="SHA256 канонической строки (64 hex chars)",
    )
    hash_business_key: Series[str] = Field(
        nullable=False,
        regex=r'^[0-9a-f]{64}$',
        description="SHA256 бизнес-ключа (64 hex chars)",
    )

    # Системные поля
    pipeline_version: Series[str] = Field(nullable=False, description="Версия пайплайна")
    run_id: Series[str] = Field(nullable=False, description="Идентификатор запуска пайплайна")
    source_system: Series[str] = Field(nullable=False, description="Источник данных")
    chembl_release: Series[str] = Field(nullable=True, description="Версия ChEMBL")
    extracted_at: Series[str] = Field(nullable=False, description="ISO8601 UTC метка времени")

    class Config:
        strict = True
        coerce = True
        ordered = False  # Column order проверяется и обеспечивается на этапе финализации

    def __init_subclass__(cls, **kwargs: Any) -> None:  # pragma: no cover - executed on subclass creation
        cast("type[Any]", super()).__init_subclass__(**kwargs)
        # Ensure Config exposes column_order via descriptor, unless overridden explicitly.
        if getattr(cls.Config, "column_order", None) is None or not isinstance(
            cls.Config.__dict__.get("column_order"), _ColumnOrderAccessor
        ):
            expose_config_column_order(cls)

    @classmethod
    def validate(
        cls,
        check_obj: pd.DataFrame,
        head: int | None = None,
        tail: int | None = None,
        sample: int | None = None,
        random_state: int | None = None,
        lazy: bool = False,
        inplace: bool = False,
    ) -> DataFrame[BaseSchema]:
        """Validate ``check_obj`` ensuring the column accessor stays patched."""

        if not isinstance(cls.Config.__dict__.get("column_order"), _ColumnOrderAccessor):
            expose_config_column_order(cls)

        return cast(
            DataFrame[BaseSchema],
            super().validate(
                check_obj,
                head=head,
                tail=tail,
                sample=sample,
                random_state=random_state,
                lazy=lazy,
                inplace=inplace,
            ),
        )

    @classmethod
    def get_column_order(cls) -> list[str]:
        """Return schema column order if defined."""

        order: list[str] | None = getattr(cls, "_column_order", None)
        return list(order) if order else []

