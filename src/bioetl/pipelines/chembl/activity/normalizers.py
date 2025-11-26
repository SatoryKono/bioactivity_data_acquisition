from __future__ import annotations

"""Normalization logic for ChEMBL activity records."""

import pandas as pd

from bioetl.schemas.activity_schema import ActivityColumns, ActivitySchema
from bioetl.sources.chembl.common import BaseChemblNormalizer, ColumnNormalizationSpec


_ACTIVITY_NORMALIZER = BaseChemblNormalizer(
    business_key_column="activity_id",
    schema=ActivitySchema,
    columns=ActivityColumns,
    column_specs=[
        ColumnNormalizationSpec("activity_id", dtype="string"),
        ColumnNormalizationSpec("assay_id", dtype="string"),
        ColumnNormalizationSpec("target_id", dtype="string", default=pd.NA),
        ColumnNormalizationSpec(
            "value", dtype="float", transformer=lambda s: pd.to_numeric(s, errors="coerce")
        ),
        ColumnNormalizationSpec("unit", dtype="string", default=pd.NA),
    ],
)


class ActivityNormalizer:
    """Apply domain normalization and schema alignment for activity rows."""

    def normalize(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        return _ACTIVITY_NORMALIZER.normalize(df_raw)


__all__ = ["ActivityNormalizer"]
