from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, overload

__all__ = [
    "Timestamp",
    "Timedelta",
    "Series",
    "DataFrame",
    "Index",
    "NA",
    "NAType",
    "concat",
    "isna",
    "notna",
    "to_numeric",
    "read_csv",
    "array",
]


class NAType:
    ...


NA: NAType


class _LocIndexer:
    def __getitem__(self, key: Any) -> Any:
        ...

    def __setitem__(self, key: Any, value: Any) -> None:
        ...


class _ILocIndexer:
    def __getitem__(self, key: Any) -> Any:
        ...


class Index(Sequence[Any]):
    @overload
    def __getitem__(self, index: int) -> Any:
        ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[Any]:
        ...

    def __len__(self) -> int:
        ...

    def tolist(self) -> list[Any]:
        ...

    def duplicated(self, *args: Any, **kwargs: Any) -> Series:
        ...

    @property
    def has_duplicates(self) -> bool:
        ...


class Series:
    name: str | None
    dtype: Any
    index: Index
    empty: bool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def __getattr__(self, name: str) -> Any:
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

    def apply(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def isna(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def get(self, key: Any, default: Any = ...) -> Any:
        ...

    def __iter__(self) -> Iterator[Any]:
        ...

    def __len__(self) -> int:
        ...

    def __getitem__(self, key: Any) -> Any:
        ...

    def __and__(self, other: Any) -> Series:
        ...

    def __lt__(self, other: Any) -> Series:
        ...

    def __le__(self, other: Any) -> Series:
        ...

    def __gt__(self, other: Any) -> Series:
        ...

    def __ge__(self, other: Any) -> Series:
        ...

    def __mod__(self, other: Any) -> Series:
        ...

    def __invert__(self) -> Series:
        ...

    def abs(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def sum(self, *args: Any, **kwargs: Any) -> Any:
        ...

    def isin(self, values: Iterable[Any]) -> Series:
        ...

    def head(self, n: int = ...) -> Series:
        ...

    def to_numpy(self, *args: Any, **kwargs: Any) -> Any:
        ...

    @property
    def str(self) -> Any:
        ...

    @property
    def loc(self) -> _LocIndexer:
        ...

    @property
    def iloc(self) -> _ILocIndexer:
        ...


class DataFrame:
    columns: Sequence[Any]
    index: Index
    empty: bool
    shape: tuple[int, int]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def __getattr__(self, name: str) -> Any:
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

    def iterrows(self, *args: Any, **kwargs: Any) -> Iterable[tuple[Any, Series]]:
        ...

    def get(self, key: Any, default: Any = ...) -> Any:
        ...

    def to_parquet(self, *args: Any, **kwargs: Any) -> None:
        ...

    def to_csv(self, *args: Any, **kwargs: Any) -> None:
        ...

    def to_json(self, *args: Any, **kwargs: Any) -> str:
        ...

    def reindex(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def reset_index(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def set_index(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def drop_duplicates(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def drop(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def merge(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def rename(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def add_prefix(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def select_dtypes(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def sort_values(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def to_dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        ...

    def describe(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def memory_usage(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def duplicated(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def isna(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def corr(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def transpose(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def sum(self, *args: Any, **kwargs: Any) -> Series:
        ...

    def round(self, *args: Any, **kwargs: Any) -> DataFrame:
        ...

    def head(self, n: int = ...) -> DataFrame:
        ...

    def __getitem__(self, key: Any) -> Any:
        ...

    def __setitem__(self, key: Any, value: Any) -> None:
        ...

    def __len__(self) -> int:
        ...

    def __iter__(self) -> Iterator[str]:
        ...

    @property
    def loc(self) -> _LocIndexer:
        ...

    @property
    def iloc(self) -> _ILocIndexer:
        ...

    @property
    def at(self) -> _LocIndexer:
        ...


class Timestamp:
    @classmethod
    def now(cls, tz: Any | None = ...) -> Timestamp:
        ...

    def isoformat(self) -> str:
        ...


class Timedelta:
    def isoformat(self) -> str:
        ...


def concat(objs: Sequence[Any], axis: int = ..., *args: Any, **kwargs: Any) -> DataFrame:
    ...


def isna(obj: Any) -> bool:
    ...


def notna(obj: Any) -> bool:
    ...


def to_numeric(*args: Any, **kwargs: Any) -> Series:
    ...


def read_csv(*args: Any, **kwargs: Any) -> DataFrame:
    ...


def array(*args: Any, **kwargs: Any) -> Any:
    ...


def TimedeltaIndex(*args: Any, **kwargs: Any) -> Index:
    ...


def date_range(*args: Any, **kwargs: Any) -> Index:
    ...
