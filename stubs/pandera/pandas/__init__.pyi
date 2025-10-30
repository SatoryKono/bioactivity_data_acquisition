from __future__ import annotations

from typing import Any, Mapping

from pandera.typing import Series


def Field(
    *,
    nullable: bool | None = ...,
    description: str | None = ...,
    regex: str | None = ...,
    ge: float | int | None = ...,
    le: float | int | None = ...,
    gt: float | int | None = ...,
    lt: float | int | None = ...,
    coerce: bool | None = ...,
) -> Series[Any]:
    ...


class DataFrameSchema:
    columns: Mapping[str, Any]
    _ordered: bool

    def validate(self, check_obj: Any, *args: Any, **kwargs: Any) -> Any:
        ...


class DataFrameModel:
    Config: type[Any]
    __extras__: Mapping[str, Any]

    @classmethod
    def to_schema(cls) -> DataFrameSchema:
        ...

    @classmethod
    def validate(cls, check_obj: Any, *args: Any, **kwargs: Any) -> Any:
        ...


__all__ = ["DataFrameModel", "DataFrameSchema", "Field"]
