"""Typed accessors for :mod:`pandera.pandas`.

This module provides a narrow, type-friendly facade over ``pandera.pandas`` so
that static type checkers can understand the attributes we rely on throughout
the codebase.  Directly importing ``pandera.pandas`` inside application modules
results in ``Module has no attribute`` errors under tools like mypy and
pyright, because the upstream package does not ship comprehensive type stubs.

By funnelling imports through this module we centralise the casting logic and
avoid scattering ``# type: ignore`` comments.  At runtime the exported ``pa``
object is simply the original ``pandera.pandas`` module, so existing behaviour
remains unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, cast

import pandera.pandas as _pandera_pandas

from bioetl.pandera_typing import Series


class _FieldCallable(Protocol):
    """Typed signature for :func:`pandera.pandas.Field`."""

    def __call__(
        self,
        *,
        nullable: bool | None = ...,
        description: str | None = ...,
        regex: str | None = ...,
        ge: float | int | None = ...,
        le: float | int | None = ...,
        gt: float | int | None = ...,
        lt: float | int | None = ...,
        coerce: bool | None = ...,
    ) -> Series[Any]:  # pragma: no cover - signature only used for typing.
        ...


class _DataFrameSchemaProtocol(Protocol):
    """Subset of ``pandera.pandas.DataFrameSchema`` used in the codebase."""

    columns: Mapping[str, Any]
    _ordered: bool

    def validate(self, check_obj: Any, *args: Any, **kwargs: Any) -> Any:
        ...


class _DataFrameModelProtocol(Protocol):
    """Shape of ``pandera.pandas.DataFrameModel`` required by schemas."""

    Config: type[Any]
    __extras__: Mapping[str, Any]

    @classmethod
    def to_schema(cls) -> _DataFrameSchemaProtocol:
        ...

    @classmethod
    def validate(cls, check_obj: Any, *args: Any, **kwargs: Any) -> Any:
        ...


class _PanderaPandasModule(Protocol):
    """Protocol describing the subset of ``pandera.pandas`` we depend on."""

    DataFrameModel: type[_DataFrameModelProtocol]
    DataFrameSchema: type[_DataFrameSchemaProtocol]
    Field: _FieldCallable


pa: _PanderaPandasModule = cast(_PanderaPandasModule, _pandera_pandas)
DataFrameModel = cast(type[_DataFrameModelProtocol], _pandera_pandas.DataFrameModel)
DataFrameSchema = cast(type[_DataFrameSchemaProtocol], _pandera_pandas.DataFrameSchema)
Field = cast(_FieldCallable, _pandera_pandas.Field)

__all__ = ["pa", "DataFrameModel", "DataFrameSchema", "Field"]

