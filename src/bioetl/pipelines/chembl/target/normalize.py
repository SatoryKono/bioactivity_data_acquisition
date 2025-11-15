"""Normalization shim for the ChEMBL target pipeline.

The current target pipeline performs identifier normalization directly inside
the transform stage.  This module exposes a dedicated entry point so that the
stage layout matches the deterministic policy (extract → transform → validate
→ normalize → write).
"""

from __future__ import annotations

import pandas as pd

__all__ = ["normalize"]


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Return ``df`` unchanged; placeholder for future normalization logic."""

    return df

