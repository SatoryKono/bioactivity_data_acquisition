"""Pandera schemas describing the raw API payload."""

from __future__ import annotations

import importlib.util

import pandas as pd

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa
from pandera.typing import Series


class RawBioactivitySchema(pa.DataFrameModel):
    """Schema for raw bioactivity records fetched from the API."""

    # Основные поля из ChEMBL API
    molecule_chembl_id: Series[str] = pa.Field(nullable=True)  # compound_id в ChEMBL
    target_pref_name: Series[str] = pa.Field(nullable=True)
    standard_value: Series[float] = pa.Field(nullable=True)  # activity_value в ChEMBL
    standard_units: Series[str] = pa.Field(nullable=True)  # activity_units в ChEMBL
    canonical_smiles: Series[str] = pa.Field(nullable=True)  # smiles в ChEMBL
    source: Series[str]
    retrieved_at: Series[pd.Timestamp]  # type: ignore[type-var]
    
    # Дополнительные поля из ChEMBL API (могут быть пустыми)
    activity_id: Series[int] = pa.Field(nullable=True)
    assay_chembl_id: Series[str] = pa.Field(nullable=True)
    document_chembl_id: Series[str] = pa.Field(nullable=True)
    standard_type: Series[str] = pa.Field(nullable=True)
    standard_relation: Series[str] = pa.Field(nullable=True)
    target_chembl_id: Series[str] = pa.Field(nullable=True)
    target_organism: Series[str] = pa.Field(nullable=True)
    target_tax_id: Series[str] = pa.Field(nullable=True)  # Может содержать None, поэтому str

    class Config:
        strict = False  # Разрешаем дополнительные колонки
        coerce = True


__all__ = ["RawBioactivitySchema"]
