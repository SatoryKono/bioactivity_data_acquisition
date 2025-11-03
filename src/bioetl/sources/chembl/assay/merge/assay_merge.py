"""Merge helpers for assay pipeline dataframes."""

from __future__ import annotations

from typing import Iterable

import pandas as pd


class AssayMergeService:
    """Combine base assay rows with expanded nested structures."""

    def merge_frames(
        self,
        base_df: pd.DataFrame,
        *expanded_frames: Iterable[pd.DataFrame],
    ) -> pd.DataFrame:
        frames = [base_df]
        for frame in expanded_frames:
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                frames.append(frame)
        if len(frames) == 1:
            return base_df
        return pd.concat(frames, ignore_index=True, sort=False)
