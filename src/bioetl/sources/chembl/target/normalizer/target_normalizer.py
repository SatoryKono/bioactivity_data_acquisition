from __future__ import annotations

import pandas as pd

from bioetl.schemas.target_schema import TargetColumns, TargetSchema
from bioetl.sources.chembl.common import BaseChemblNormalizer, ColumnNormalizationSpec


_TARGET_NORMALIZER = BaseChemblNormalizer(
    business_key_column="target_id",
    schema=TargetSchema,
    columns=TargetColumns,
    column_specs=[
        ColumnNormalizationSpec("target_id", dtype="string"),
        ColumnNormalizationSpec("pref_name", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("organism", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("target_type", dtype="string", default=pd.NA),
    ],
)


def normalize_target(df_raw: pd.DataFrame) -> pd.DataFrame:
    return _TARGET_NORMALIZER.normalize(df_raw)
