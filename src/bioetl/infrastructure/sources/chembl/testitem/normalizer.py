from __future__ import annotations

import pandas as pd

from bioetl.core.schemas.testitem_schema import (
    TestItemColumns,
    TestItemSchema,
)
from bioetl.infrastructure.sources.chembl.common import (
    BaseChemblNormalizer,
    ColumnNormalizationSpec,
)


_TESTITEM_NORMALIZER = BaseChemblNormalizer(
    business_key_column="test_item_id",
    schema=TestItemSchema,
    columns=TestItemColumns,
    column_specs=[
        ColumnNormalizationSpec("test_item_id", dtype="string"),
        ColumnNormalizationSpec("name", dtype="string", default=pd.NA),
        ColumnNormalizationSpec(
            "molecule_type", dtype="string", default=pd.NA
        ),
        ColumnNormalizationSpec("inchi_key", dtype="string", default=pd.NA),
    ],
)


def normalize_testitem(df_raw: pd.DataFrame) -> pd.DataFrame:
    return _TESTITEM_NORMALIZER.normalize(df_raw)
