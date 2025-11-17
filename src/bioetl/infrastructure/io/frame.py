from __future__ import annotations

import pandas as pd
from pandas import Series


def ensure_columns(df: pd.DataFrame, columns: tuple[tuple[str, str], ...]) -> pd.DataFrame:
    """Ensure that columns with the specified data types exist in the DataFrame.

    Args:
        df: Input DataFrame.
        columns: Tuple of pairs ``(column name, pandas dtype)``.

    Returns:
        Copy of the DataFrame with missing columns added.
    """
    out = df.copy()

    for name, dtype in columns:
        if name not in out.columns:
            out[name] = Series(pd.NA, index=out.index, dtype=dtype)

    return out

