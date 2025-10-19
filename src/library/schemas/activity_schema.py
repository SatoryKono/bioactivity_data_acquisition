"""Pandera схемы для валидации данных activity (raw и normalized)."""

from __future__ import annotations

import importlib.util

import pandas as pd
from pandera.typing import Series

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa


class RawActivitySchema(pa.DataFrameModel):
    """Схема для сырых записей активности, загружаемых из API."""

    # Технические служебные поля
    source_system: Series[str] = pa.Field(nullable=False, description="Система-источник (ChEMBL)")
    chembl_release: Series[str] = pa.Field(nullable=True, description="Версия ChEMBL")

    # Обязательные служебные поля
    source: Series[str] = pa.Field(nullable=False)
    retrieved_at: Series[pd.Timestamp] = pa.Field(nullable=False)

    # Основные поля активности
    target_pref_name: Series[str] = pa.Field(nullable=True)
    standard_value: Series[float] = pa.Field(nullable=True)
    standard_units: Series[str] = pa.Field(nullable=True)
    canonical_smiles: Series[str] = pa.Field(nullable=True)

    # ChEMBL-специфика
    activity_id: Series[int] = pa.Field(nullable=True)
    assay_chembl_id: Series[str] = pa.Field(nullable=True)
    document_chembl_id: Series[str] = pa.Field(nullable=True)
    standard_type: Series[str] = pa.Field(nullable=True)
    standard_relation: Series[str] = pa.Field(nullable=True)
    target_chembl_id: Series[str] = pa.Field(nullable=True)
    target_organism: Series[str] = pa.Field(nullable=True)
    target_tax_id: Series[str] = pa.Field(nullable=True)

    # Дополнительные поля из ChEMBL API (только те, что реально есть в данных)
    action_type: Series[str] = pa.Field(nullable=True)
    activity_comment: Series[str] = pa.Field(nullable=True)
    activity_properties: Series[object] = pa.Field(nullable=True)  # список
    assay_description: Series[str] = pa.Field(nullable=True)
    assay_type: Series[str] = pa.Field(nullable=True)
    assay_variant_accession: Series[str] = pa.Field(nullable=True)
    assay_variant_mutation: Series[str] = pa.Field(nullable=True)
    bao_endpoint: Series[str] = pa.Field(nullable=True)
    bao_format: Series[str] = pa.Field(nullable=True)
    bao_label: Series[str] = pa.Field(nullable=True)
    data_validity_comment: Series[str] = pa.Field(nullable=True)
    data_validity_description: Series[str] = pa.Field(nullable=True)
    document_journal: Series[str] = pa.Field(nullable=True)
    document_year: Series[int] = pa.Field(nullable=True)
    ligand_efficiency: Series[str] = pa.Field(nullable=True)
    molecule_chembl_id: Series[str] = pa.Field(nullable=True)
    molecule_pref_name: Series[str] = pa.Field(nullable=True)
    parent_molecule_chembl_id: Series[str] = pa.Field(nullable=True)
    pchembl_value: Series[float] = pa.Field(nullable=True)
    potential_duplicate: Series[int] = pa.Field(nullable=True)  # 0/1 как int
    qudt_units: Series[str] = pa.Field(nullable=True)
    record_id: Series[int] = pa.Field(nullable=True)
    relation: Series[str] = pa.Field(nullable=True)
    src_id: Series[int] = pa.Field(nullable=True)
    standard_flag: Series[int] = pa.Field(nullable=True)  # 0/1 как int
    standard_text_value: Series[str] = pa.Field(nullable=True)
    standard_upper_value: Series[float] = pa.Field(nullable=True)
    text_value: Series[str] = pa.Field(nullable=True)
    toid: Series[str] = pa.Field(nullable=True)  # приходит как строка, не int
    type: Series[str] = pa.Field(nullable=True)
    uo_units: Series[str] = pa.Field(nullable=True)
    upper_value: Series[float] = pa.Field(nullable=True)
    value: Series[float] = pa.Field(nullable=True)
    units: Series[str] = pa.Field(nullable=True)

    # Хеш-поля для дедупликации
    hash_row: Series[str] = pa.Field(nullable=True, description="Хеш строки для дедупликации")
    hash_business_key: Series[str] = pa.Field(nullable=True, description="Хеш бизнес-ключа (activity_id)")

    class Config:
        strict = False  # Разрешаем дополнительные колонки от ChEMBL API
        coerce = True


class NormalizedActivitySchema(pa.DataFrameModel):
    """Схема для нормализованных таблиц активности (готовых к экспорту)."""

    # Технические служебные поля
    source_system: Series[str] = pa.Field(nullable=False, description="Система-источник (ChEMBL)")
    chembl_release: Series[str] = pa.Field(nullable=False, description="Версия ChEMBL")

    # Обязательные служебные поля
    source: Series[str] = pa.Field(nullable=False)
    retrieved_at: Series[pd.Timestamp] = pa.Field(nullable=False)

    # Нормализованные поля
    target: Series[str] = pa.Field(nullable=True)
    activity_value: Series[float] = pa.Field(nullable=True)
    activity_unit: Series[str] = pa.Field(nullable=True)
    smiles: Series[str] = pa.Field(nullable=True)

    # Дополнительные поля, которые остаются после нормализации
    activity_id: Series[int] = pa.Field(nullable=True)
    assay_chembl_id: Series[str] = pa.Field(nullable=True)
    document_chembl_id: Series[str] = pa.Field(nullable=True)
    standard_type: Series[str] = pa.Field(nullable=True)
    standard_relation: Series[str] = pa.Field(nullable=True)
    target_chembl_id: Series[str] = pa.Field(nullable=True)
    target_organism: Series[str] = pa.Field(nullable=True)
    target_tax_id: Series[str] = pa.Field(nullable=True)

    # Дополнительные поля из ChEMBL API
    action_type: Series[str] = pa.Field(nullable=True)
    activity_comment: Series[str] = pa.Field(nullable=True)
    activity_properties: Series[object] = pa.Field(nullable=True)
    assay_description: Series[str] = pa.Field(nullable=True)
    assay_type: Series[str] = pa.Field(nullable=True)
    assay_variant_accession: Series[str] = pa.Field(nullable=True)
    assay_variant_mutation: Series[str] = pa.Field(nullable=True)
    bao_endpoint: Series[str] = pa.Field(nullable=True)
    bao_format: Series[str] = pa.Field(nullable=True)
    bao_label: Series[str] = pa.Field(nullable=True)
    data_validity_comment: Series[str] = pa.Field(nullable=True)
    data_validity_description: Series[str] = pa.Field(nullable=True)
    document_journal: Series[str] = pa.Field(nullable=True)
    document_year: Series[int] = pa.Field(nullable=True)
    ligand_efficiency: Series[str] = pa.Field(nullable=True)
    molecule_chembl_id: Series[str] = pa.Field(nullable=True)
    molecule_pref_name: Series[str] = pa.Field(nullable=True)
    parent_molecule_chembl_id: Series[str] = pa.Field(nullable=True)
    pchembl_value: Series[float] = pa.Field(nullable=True)
    potential_duplicate: Series[int] = pa.Field(nullable=True)
    qudt_units: Series[str] = pa.Field(nullable=True)
    record_id: Series[int] = pa.Field(nullable=True)
    relation: Series[str] = pa.Field(nullable=True)
    src_id: Series[int] = pa.Field(nullable=True)
    standard_flag: Series[int] = pa.Field(nullable=True)
    standard_text_value: Series[str] = pa.Field(nullable=True)
    standard_upper_value: Series[float] = pa.Field(nullable=True)
    text_value: Series[str] = pa.Field(nullable=True)
    toid: Series[str] = pa.Field(nullable=True)
    type: Series[str] = pa.Field(nullable=True)
    uo_units: Series[str] = pa.Field(nullable=True)
    upper_value: Series[float] = pa.Field(nullable=True)
    value: Series[float] = pa.Field(nullable=True)
    units: Series[str] = pa.Field(nullable=True)

    # Хеш-поля для дедупликации
    hash_row: Series[str] = pa.Field(nullable=False, description="Хеш строки для дедупликации")
    hash_business_key: Series[str] = pa.Field(nullable=False, description="Хеш бизнес-ключа (activity_id)")

    class Config:
        strict = False  # Разрешаем дополнительные колонки
        coerce = True


__all__ = ["RawActivitySchema", "NormalizedActivitySchema"]
