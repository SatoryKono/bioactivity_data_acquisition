"""I/O helpers for reading inputs and writing outputs deterministically."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from .normalize import coerce_text


def read_input_csv(path: Path) -> pd.DataFrame:
    """Read the input CSV file forcing string dtype for identifier columns."""

    df = pd.read_csv(path, dtype=str).fillna("")
    for column in df.columns:
        df[column] = df[column].apply(coerce_text)
    return df


def write_output_csv(df: pd.DataFrame, path: Path, columns: Optional[Iterable[str]] = None) -> None:
    """Write a DataFrame to CSV deterministically by ordering columns and rows."""

    path.parent.mkdir(parents=True, exist_ok=True)
    ordered_df = df.copy()
    if columns is not None:
        missing = [c for c in columns if c not in ordered_df.columns]
        for column in missing:
            ordered_df[column] = None
        ordered_df = ordered_df.loc[:, list(columns)]
    ordered_df.to_csv(path, index=False, encoding="utf-8", na_rep="", line_terminator="\n")

