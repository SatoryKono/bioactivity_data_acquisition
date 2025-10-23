# Pandera-патч для testitem
# Сгенерировано: 2025-10-23 22:49:58

from pandera import pa
from pandera.typing import Series
import pandas as pd

class TestitemNormalizedSchemaPatch(pa.DataFrameModel):
    """Дополнительные поля для нормализованной схемы."""

    chembl_release.1: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для chembl_release.1"
    )

    extracted_at.1: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для extracted_at.1"
    )

    hash_business_key.1: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для hash_business_key.1"
    )

    hash_row.1: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для hash_row.1"
    )

    index.1: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для index.1"
    )

    pipeline_version.1: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для pipeline_version.1"
    )

    source_system.1: Series[str] = pa.Field(
        nullable=True,
        description="TODO: Добавить описание для source_system.1"
    )

    class Config:
        strict = True
        coerce = True
