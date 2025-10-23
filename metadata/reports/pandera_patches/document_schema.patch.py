# Pandera-патч для document
# Сгенерировано: 2025-10-23 22:49:58

from pandera import pa
from pandera.typing import Series
import pandas as pd

class DocumentNormalizedSchemaPatch(pa.DataFrameModel):
    """Дополнительные поля для нормализованной схемы."""

    class Config:
        strict = True
        coerce = True
