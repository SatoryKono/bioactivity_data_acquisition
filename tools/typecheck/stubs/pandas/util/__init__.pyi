from __future__ import annotations

from typing import Any
from collections.abc import Hashable, Iterable

from pandas import Series


def hash_pandas_object(
    obj: Series | Iterable[Hashable],
    index: bool = ...,
    *args: Any,
    **kwargs: Any,
) -> Series:
    ...
