"""Base Pandera schemas."""

from typing import Any

import pandera as pa
from pandera.typing import Series


class BaseSchema(pa.DataFrameModel):
    """Базовый класс для Pandera схем."""

    # Системные поля
    pipeline_version: str = pa.Field(nullable=False)
    source_system: str = pa.Field(nullable=False)
    chembl_release: str | None = pa.Field(nullable=True)
    extracted_at: str = pa.Field(nullable=False)  # ISO8601 UTC

    class Config:
        strict = True
        coerce = True
        ordered = False  # Disable column order check for now

