# Pandera-патч для activity
# Сгенерировано: 2025-10-23 22:49:58

from pandera import pa
from pandera.typing import Series
import pandas as pd

class ActivityNormalizedSchemaPatch(pa.DataFrameModel):
    """Дополнительные поля для нормализованной схемы."""

    saltform_id: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для saltform_id"
    )

    class Config:
        strict = True
        coerce = True
