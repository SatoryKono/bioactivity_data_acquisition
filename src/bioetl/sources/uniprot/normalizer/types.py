"""Shared types for UniProt normalization/enrichment."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(slots=True)
class UniProtEnrichmentResult:
    """Container holding UniProt enrichment artefacts.

    Notes:
        - All dataframes MUST be `convert_dtypes()`-normalized upstream.
        - Field order is stable to preserve deterministic serialization.
    """

    dataframe: pd.DataFrame
    silver: pd.DataFrame
    components: pd.DataFrame
    metrics: dict[str, Any]
    missing_mappings: list[dict[str, Any]] = field(default_factory=list)
    validation_issues: list[dict[str, Any]] = field(default_factory=list)


