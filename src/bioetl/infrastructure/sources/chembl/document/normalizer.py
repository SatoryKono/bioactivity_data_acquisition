from __future__ import annotations

import pandas as pd

from bioetl.core.schemas.document_schema import DocumentColumns, DocumentSchema
from bioetl.infrastructure.sources.chembl.common import BaseChemblNormalizer, ColumnNormalizationSpec


_DOCUMENT_NORMALIZER = BaseChemblNormalizer(
    business_key_column="document_id",
    schema=DocumentSchema,
    columns=DocumentColumns,
    column_specs=[
        ColumnNormalizationSpec("document_id", dtype="string"),
        ColumnNormalizationSpec("doi", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("title", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("journal", dtype="string", default=pd.NA),
    ],
)


def normalize_document(df_raw: pd.DataFrame) -> pd.DataFrame:
    return _DOCUMENT_NORMALIZER.normalize(df_raw)
