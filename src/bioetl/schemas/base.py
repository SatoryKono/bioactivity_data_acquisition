"""Base Pandera schemas and shared helpers."""

from __future__ import annotations

import pandera.pandas as pa
from pandera.typing import Series


class _ColumnOrderAccessor:
    """Descriptor exposing schema ``column_order`` without Pandera side-effects."""

    def __init__(self, schema_cls: type["BaseSchema"]):
        self._schema_cls = schema_cls

    def __get__(self, instance, owner):  # noqa: D401 - standard descriptor signature
        schema_cls = getattr(owner, "_schema_cls", self._schema_cls)
        return schema_cls.get_column_order()


def expose_config_column_order(schema_cls: type["BaseSchema"]) -> None:
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
    schema_cls.Config._schema_cls = schema_cls  # type: ignore[attr-defined]
    setattr(schema_cls.Config, "column_order", accessor)

    extras = getattr(schema_cls, "__extras__", None)
    if isinstance(extras, dict) and "column_order" in extras:
        # Pandera inspects ``__extras__`` to register dataframe-level checks.
        # Remove ``column_order`` so the descriptor isn't treated as a Check.
        updated = dict(extras)
        updated.pop("column_order", None)
        schema_cls.__extras__ = updated  # type: ignore[attr-defined]


class BaseSchema(pa.DataFrameModel):
    """Базовый класс для Pandera схем.

    Содержит обязательные системные поля для всех пайплайнов:
    - index: детерминированный индекс строки
    - hash_row: SHA256 от канонической строки (для проверки целостности)
    - hash_business_key: SHA256 от бизнес-ключа (для дедупликации)
    - pipeline_version: версия пайплайна
    - source_system: источник данных
    - chembl_release: версия ChEMBL
    - extracted_at: метка времени извлечения (ISO8601)
    """

    # Детерминизм и система трекинга
    index: Series[int] = pa.Field(nullable=False, ge=0, description="Детерминированный индекс строки")
    hash_row: Series[str] = pa.Field(
        nullable=False,
        regex=r'^[0-9a-f]{64}$',
        description="SHA256 канонической строки (64 hex chars)",
    )
    hash_business_key: Series[str] = pa.Field(
        nullable=False,
        regex=r'^[0-9a-f]{64}$',
        description="SHA256 бизнес-ключа (64 hex chars)",
    )

    # Системные поля
    pipeline_version: Series[str] = pa.Field(nullable=False, description="Версия пайплайна")
    source_system: Series[str] = pa.Field(nullable=False, description="Источник данных")
    chembl_release: Series[str] = pa.Field(nullable=True, description="Версия ChEMBL")
    extracted_at: Series[str] = pa.Field(nullable=False, description="ISO8601 UTC метка времени")

    class Config:
        strict = True
        coerce = True
        ordered = True  # Enforce column order

    def __init_subclass__(cls, **kwargs):  # pragma: no cover - executed on subclass creation
        super().__init_subclass__(**kwargs)
        # Ensure Config exposes column_order via descriptor, unless overridden explicitly.
        if getattr(cls.Config, "column_order", None) is None or not isinstance(
            cls.Config.__dict__.get("column_order"), _ColumnOrderAccessor
        ):
            expose_config_column_order(cls)

    @classmethod
    def get_column_order(cls) -> list[str]:
        """Return schema column order if defined."""

        order = getattr(cls, "_column_order", None)
        return list(order) if order else []

