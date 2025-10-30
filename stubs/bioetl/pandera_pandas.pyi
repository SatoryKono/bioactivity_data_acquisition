from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from pandera.typing import Series as _Series

class _FieldCallable(Protocol):
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
    ) -> _Series[Any]:
        ...


class _DataFrameSchemaProtocol(Protocol):
    columns: Mapping[str, Any]
    _ordered: bool

    def validate(self, check_obj: Any, *args: Any, **kwargs: Any) -> Any:
        ...


class DataFrameModel:
    Config: type[Any]
    __extras__: Mapping[str, Any]

    @classmethod
    def to_schema(cls) -> _DataFrameSchemaProtocol:
        ...

    @classmethod
    def validate(cls, check_obj: Any, *args: Any, **kwargs: Any) -> Any:
        ...


class _PanderaPandasModule(Protocol):
    DataFrameModel: type[DataFrameModel]
    DataFrameSchema: type[_DataFrameSchemaProtocol]
    Field: _FieldCallable


pa: _PanderaPandasModule
DataFrameSchema: type[_DataFrameSchemaProtocol]
Field: _FieldCallable

__all__ = ["pa", "DataFrameModel", "DataFrameSchema", "Field"]
