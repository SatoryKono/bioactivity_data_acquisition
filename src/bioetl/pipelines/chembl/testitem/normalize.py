"""Normalization shim for the ChEMBL test item pipeline."""

from __future__ import annotations

import pandas as pd

__all__ = ["normalize"]


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Return ``df`` unchanged; placeholder for future normalization logic."""

    return df

