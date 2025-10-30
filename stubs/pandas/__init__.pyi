from __future__ import annotations

from typing import Any, Iterable, Iterator, Sequence

class Int64Dtype:
    ...


class BooleanDtype:
    ...


class _LocIndexer:
    def __getitem__(self, key: Any) -> Any:
        ...

    def __setitem__(self, key: Any, value: Any) -> None:
        ...


class _ILocIndexer:
    def __getitem__(self, key: Any) -> Any:
        ...


class Series:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def map(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def bfill(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def dropna(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def astype(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def tolist(self) -> list[Any]:
        ...

    def notna(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def any(self, *args: Any, **kwargs: Any) -> bool:
        ...

    def max(self, *args: Any, **kwargs: Any) -> Any:
        ...

    def fillna(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def where(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def __iter__(self) -> Iterator[Any]:
        ...

    @property
    def iloc(self) -> _ILocIndexer:
        ...


class DataFrame:
    columns: Sequence[Any]
    index: Sequence[Any]
    empty: bool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def copy(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def bfill(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def apply(self, *args: Any, **kwargs: Any) -> Any:
        ...

    def any(self, *args: Any, **kwargs: Any) -> bool:
        ...

    def dropna(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def fillna(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def groupby(self, *args: Any, **kwargs: Any) -> Any:
        ...

    def astype(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def notna(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def where(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def convert_dtypes(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def itertuples(self, *args: Any, **kwargs: Any) -> Iterable[Any]:
        ...

    def get(self, key: Any, default: Any = ...) -> Any:
        ...

    def to_parquet(self, *args: Any, **kwargs: Any) -> None:
        ...

    def to_csv(self, *args: Any, **kwargs: Any) -> None:
        ...

    def reindex(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def __getitem__(self, key: Any) -> Any:
        ...

    def __setitem__(self, key: Any, value: Any) -> None:
        ...

    @property
    def loc(self) -> _LocIndexer:
        ...

    @property
    def iloc(self) -> _ILocIndexer:
        ...


class NAType:
    ...


NA: NAType


def concat(objs: Sequence[Any], axis: int = ..., *args: Any, **kwargs: Any) -> DataFrame:
    ...


def isna(obj: Any) -> bool:
    ...


def to_numeric(*args: Any, **kwargs: Any) -> Series:
    ...
