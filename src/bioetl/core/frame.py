from __future__ import annotations

import pandas as pd
from pandas import Series


def ensure_columns(df: pd.DataFrame, columns: tuple[tuple[str, str], ...]) -> pd.DataFrame:
    """Обеспечить наличие колонок с заданными типами данных.

    Args:
        df: Входной DataFrame.
        columns: Кортеж пар (имя колонки, pandas dtype).

    Returns:
        Копия DataFrame с добавленными отсутствующими колонками.
    """
    out = df.copy()

    for name, dtype in columns:
        if name not in out.columns:
            out[name] = Series(pd.NA, index=out.index, dtype=dtype)

    return out

