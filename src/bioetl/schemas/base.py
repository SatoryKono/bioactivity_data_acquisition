"""Base Pandera schemas."""

import pandera as pa
from pandera.typing import Series


class BaseSchema(pa.DataFrameModel):
    """Базовый класс для Pandera схем.

    Содержит обязательные системные поля для всех пайплайнов:
    - index: детерминированный индекс строки
    - hash_row: SHA256 от канонической строки (для проверки целостности)
    - hash_business_key: SHA256 от бизнес-ключа (для дедупликации)
    - pipeline_version: версия пайплайна
    - source_system: источник данных
    - chembl_release: версия ChEMBL
    - extracted_at: метка времени извлечения (ISO8601)
    """

    # Детерминизм и система трекинга
    index: Series[int] = pa.Field(nullable=False, ge=0, description="Детерминированный индекс строки")
    hash_row: Series[str] = pa.Field(
        nullable=False,
        regex=r'^[0-9a-f]{64}$',
        description="SHA256 канонической строки (64 hex chars)",
    )
    hash_business_key: Series[str] = pa.Field(
        nullable=False,
        regex=r'^[0-9a-f]{64}$',
        description="SHA256 бизнес-ключа (64 hex chars)",
    )

    # Системные поля
    pipeline_version: Series[str] = pa.Field(nullable=False, description="Версия пайплайна")
    source_system: Series[str] = pa.Field(nullable=False, description="Источник данных")
    chembl_release: Series[str] = pa.Field(nullable=True, description="Версия ChEMBL")
    extracted_at: Series[str] = pa.Field(nullable=False, description="ISO8601 UTC метка времени")

    class Config:
        strict = True
        coerce = True
        ordered = True  # Enforce column order

    @classmethod
    def get_column_order(cls) -> list[str]:
        """Return schema column order if defined."""

        order = getattr(cls, "_column_order", None)
        return list(order) if order else []

