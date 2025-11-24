"""Protocols describing repositories used by ChEMBL molecule join logic."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

import pandas as pd

__all__ = [
    "ActivityRepository",
    "CompoundRecordRepository",
    "MoleculeRepository",
]


class ActivityRepository(Protocol):
    """Repository providing access to ChEMBL activity records."""

    def fetch_by_ids(
        self, activity_ids: Sequence[str], *, fields: Sequence[str]
    ) -> pd.DataFrame:
        """Fetch activity records filtered by ``activity_id``."""


class CompoundRecordRepository(Protocol):
    """Repository exposing compound_record lookups by ``record_id``."""

    def fetch_by_record_ids(
        self, record_ids: Sequence[str]
    ) -> Mapping[str, Mapping[str, object]]:
        """Return a mapping keyed by canonical ``record_id`` values."""


class MoleculeRepository(Protocol):
    """Repository exposing molecule lookups by ``molecule_chembl_id``."""

    def fetch_by_ids(
        self,
        molecule_ids: Sequence[str],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """Return molecule metadata for the provided identifiers."""
