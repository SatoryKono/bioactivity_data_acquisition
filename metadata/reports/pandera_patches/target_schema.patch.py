# Pandera-патч для target
# Сгенерировано: 2025-10-23 22:49:58

from pandera import pa
from pandera.typing import Series
import pandas as pd

class TargetNormalizedSchemaPatch(pa.DataFrameModel):
    """Дополнительные поля для нормализованной схемы."""

    extraction_status: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для extraction_status"
    )

    ptm_disulfide_bond: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для ptm_disulfide_bond"
    )

    ptm_glycosylation: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для ptm_glycosylation"
    )

    ptm_lipidation: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для ptm_lipidation"
    )

    ptm_modified_residue: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для ptm_modified_residue"
    )

    class Config:
        strict = True
        coerce = True
