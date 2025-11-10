"""Функции для детерминированной подготовки табличных данных."""
from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

ColumnSpec = tuple[str, str]
ColumnSpecs = Iterable[ColumnSpec]


def ensure_columns(df: pd.DataFrame, columns: ColumnSpecs) -> pd.DataFrame:
    """Вернуть копию ``df`` с гарантированно присутствующими колонками указанных типов.

    Parameters
    ----------
    df:
        Исходный DataFrame, который требуется дополнить.
    columns:
        Итерация из пар ``(column_name, pandas_dtype)``. Для отсутствующих колонок будут
        созданы Series с ``pd.NA`` и указанным dtype.

    Returns
    -------
    pd.DataFrame
        Копия ``df`` с добавленными недостающими колонками.
    """

    result = df.copy()
    for name, dtype in columns:
        if name not in result.columns:
            result[name] = pd.Series(pd.NA, index=result.index, dtype=dtype)
    return result

