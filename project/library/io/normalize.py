"""Normalization helpers for pipeline inputs and outputs."""

from __future__ import annotations

from typing import Iterable

import pandas as pd


QUERY_COLUMNS = ["query", "type"]
PUBLICATION_COLUMNS = ["source", "identifier", "title", "published_at", "doi"]


def normalize_query_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with canonical column names and whitespace trimmed."""

    normalized = df.copy()
    normalized.columns = [column.strip().lower() for column in normalized.columns]
    for column in QUERY_COLUMNS:
        if column in normalized.columns:
            normalized[column] = normalized[column].astype(str).str.strip()
    return normalized.loc[:, [column for column in QUERY_COLUMNS if column in normalized.columns]]


def normalize_publication_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Deterministically order publication columns and sort the frame."""

    normalized = df.copy()
    for column in PUBLICATION_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    normalized = normalized[PUBLICATION_COLUMNS]
    if "published_at" in normalized:
        normalized["published_at"] = pd.to_datetime(normalized["published_at"], errors="coerce")
    normalized.sort_values(by=["identifier", "source"], inplace=True, ignore_index=True)
    return normalized


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Ensure that all columns exist, filling missing ones with ``pd.NA``."""

    enriched = df.copy()
    for column in columns:
        if column not in enriched.columns:
            enriched[column] = pd.NA
    return enriched
