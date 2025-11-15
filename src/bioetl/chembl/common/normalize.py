"""Normalization helpers shared across ChEMBL pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from bioetl.core.schema.normalizers import (
    IdentifierRule,
    IdentifierStats,
    normalize_identifier_columns,
)

__all__ = ["normalize_identifiers", "add_row_metadata", "RowMetadataChanges"]


@dataclass
class RowMetadataChanges:
    """Metadata describing the mutations applied by :func:`add_row_metadata`."""

    subtype_added: bool = False
    subtype_filled: bool = False
    index_added: bool = False
    index_filled: bool = False

    @property
    def has_changes(self) -> bool:
        """Return ``True`` when any metadata column was modified."""

        return any((self.subtype_added, self.subtype_filled, self.index_added, self.index_filled))


def normalize_identifiers(
    df: pd.DataFrame,
    rules: Iterable[IdentifierRule],
    *,
    copy: bool = True,
) -> tuple[pd.DataFrame, IdentifierStats]:
    """Normalize identifier columns according to ``rules``.

    Parameters
    ----------
    df:
        Input DataFrame to normalize.
    rules:
        Iterable of :class:`~bioetl.core.schema.normalizers.IdentifierRule` instances
        describing how each identifier column should be processed.
    copy:
        When ``True`` (default) operate on a copy of ``df``.

    Returns
    -------
    tuple[pd.DataFrame, IdentifierStats]
        Normalized DataFrame and aggregated normalization statistics.
    """

    materialized_rules = tuple(rules)
    if not materialized_rules:
        return (df.copy() if copy else df, IdentifierStats())

    return normalize_identifier_columns(df, materialized_rules, copy=copy)


def add_row_metadata(
    df: pd.DataFrame,
    *,
    subtype: str,
    subtype_column: str = "row_subtype",
    index_column: str = "row_index",
    copy: bool = True,
) -> tuple[pd.DataFrame, RowMetadataChanges]:
    """Ensure that standard row metadata columns are populated.

    Parameters
    ----------
    df:
        Input DataFrame.
    subtype:
        Value assigned to ``subtype_column`` when the column is created or filled.
    subtype_column:
        Name of the column storing the subtype label (defaults to ``row_subtype``).
    index_column:
        Name of the column storing the sequential row index (defaults to ``row_index``).
    copy:
        When ``True`` (default) operate on a copy of ``df``.

    Returns
    -------
    tuple[pd.DataFrame, RowMetadataChanges]
        DataFrame with metadata applied and a description of the performed mutations.
    """

    result = df.copy() if copy else df
    changes = RowMetadataChanges()

    if result.empty:
        return result, changes

    if subtype_column not in result.columns:
        result[subtype_column] = subtype
        changes.subtype_added = True
    else:
        series = result[subtype_column]
        if series.isna().all():
            result[subtype_column] = subtype
            changes.subtype_filled = True

    if index_column not in result.columns:
        result[index_column] = pd.RangeIndex(start=0, stop=len(result), step=1)
        changes.index_added = True
    else:
        series = result[index_column]
        if series.isna().all():
            result[index_column] = pd.RangeIndex(start=0, stop=len(result), step=1)
            changes.index_filled = True

    return result, changes
