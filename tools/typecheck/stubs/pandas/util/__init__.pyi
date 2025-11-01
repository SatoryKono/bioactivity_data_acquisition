from __future__ import annotations

from collections.abc import Hashable, Iterable
from typing import Any

from pandas import Series

def hash_pandas_object(
    obj: Series | Iterable[Hashable],
    index: bool = ...,
    *args: Any,
    **kwargs: Any,
) -> Series:
    ...
