from __future__ import annotations

from typing import Any, Sequence

class Int64Dtype:
    ...


class BooleanDtype:
    ...


class _LocIndexer:
    def __getitem__(self, key: Any) -> Any:
        ...


class DataFrame:
    columns: Sequence[Any]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def copy(self, *args: Any, **kwargs: Any) -> DataFrame:
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


class NAType:
    ...


NA: NAType


def concat(objs: Sequence[Any], axis: int = ..., *args: Any, **kwargs: Any) -> DataFrame:
    ...


def Series(*args: Any, **kwargs: Any) -> Any:
    ...
