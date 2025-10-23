# Pandera-патч для assay
# Сгенерировано: 2025-10-23 22:49:58

from pandera import pa
from pandera.typing import Series
import pandas as pd

class AssayNormalizedSchemaPatch(pa.DataFrameModel):
    """Дополнительные поля для нормализованной схемы."""

    assay_classifications: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для assay_classifications"
    )

    assay_type_description: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для assay_type_description"
    )

    confidence_score: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для confidence_score"
    )

    isoform: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для isoform"
    )

    mutation: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для mutation"
    )

    relationship_type: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для relationship_type"
    )

    sequence: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для sequence"
    )

    src_assay_id: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для src_assay_id"
    )

    src_id: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для src_id"
    )

    src_name: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для src_name"
    )

    class Config:
        strict = True
        coerce = True
