from __future__ import annotations

import pandas as pd

from bioetl.core.schemas.assay_schema import AssayColumns, AssaySchema
from bioetl.infrastructure.sources.chembl.common import BaseChemblNormalizer, ColumnNormalizationSpec


_ASSAY_NORMALIZER = BaseChemblNormalizer(
    business_key_column="assay_id",
    schema=AssaySchema,
    columns=AssayColumns,
    column_specs=[
        ColumnNormalizationSpec("assay_id", dtype="string"),
        ColumnNormalizationSpec("assay_type", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("description", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("target_id", dtype="string", default=pd.NA),
    ],
)


def normalize_assay(df_raw: pd.DataFrame) -> pd.DataFrame:
    return _ASSAY_NORMALIZER.normalize(df_raw)
