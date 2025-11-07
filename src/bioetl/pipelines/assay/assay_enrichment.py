"""Enrichment functions for Assay pipeline."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import pandas as pd

from bioetl.clients import ChemblClient
from bioetl.core.logger import UnifiedLogger
from bioetl.schemas.assay import (
    ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA,
    ASSAY_PARAMETERS_ENRICHMENT_SCHEMA,
)

__all__ = [
    "enrich_with_assay_classifications",
    "enrich_with_assay_parameters",
]


def _ensure_columns(
    df: pd.DataFrame,
    columns: tuple[tuple[str, str], ...],
) -> pd.DataFrame:
    result = df.copy()
    for name, dtype in columns:
        if name not in result.columns:
            result[name] = pd.Series(pd.NA, index=result.index, dtype=dtype)
    return result


_CLASSIFICATION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("assay_classifications", "string"),
    ("assay_class_id", "string"),
)

_PARAMETERS_COLUMNS: tuple[tuple[str, str], ...] = (("assay_parameters", "string"),)


def enrich_with_assay_classifications(
    df_assay: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Обогатить DataFrame assay данными из ASSAY_CLASS_MAP и ASSAY_CLASSIFICATION.

    Parameters
    ----------
    df_assay:
        DataFrame с данными assay, должен содержать assay_chembl_id.
    client:
        ChemblClient для запросов к ChEMBL API.
    cfg:
        Конфигурация обогащения из config.chembl.assay.enrich.classifications.

    Returns
    -------
    pd.DataFrame:
        Обогащенный DataFrame с добавленными/обновленными колонками:
        - assay_classifications (string, nullable) - сериализованный массив классификаций
        - assay_class_id (string, nullable) - список assay_class_id через ";"
    """
    log = UnifiedLogger.get(__name__).bind(component="assay_enrichment")

    df_assay = _ensure_columns(df_assay, _CLASSIFICATION_COLUMNS)

    if df_assay.empty:
        log.debug("enrichment_skipped_empty_dataframe")
        return ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Проверка наличия необходимых колонок
    required_cols = ["assay_chembl_id"]
    missing_cols = [col for col in required_cols if col not in df_assay.columns]
    if missing_cols:
        log.warning(
            "enrichment_skipped_missing_columns",
            missing_columns=missing_cols,
        )
        return ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Собрать уникальные assay_chembl_id, dropna
    assay_ids: list[str] = []
    for _, row in df_assay.iterrows():
        assay_id = row.get("assay_chembl_id")

        # Пропускаем NaN/None значения
        if pd.isna(assay_id) or assay_id is None:
            continue

        # Преобразуем в строку
        assay_id_str = str(assay_id).strip()

        if assay_id_str:
            assay_ids.append(assay_id_str)

    if not assay_ids:
        log.debug("enrichment_skipped_no_valid_ids")
        return ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Получить конфигурацию
    class_map_fields = cfg.get("class_map_fields", ["assay_chembl_id", "assay_class_id"])
    classification_fields = cfg.get(
        "classification_fields",
        ["assay_class_id", "l1", "l2", "l3", "pref_name"],
    )
    page_limit = cfg.get("page_limit", 1000)

    # Шаг 1: Получить ASSAY_CLASS_MAP по assay_chembl_id
    log.info("enrichment_fetching_assay_class_map", ids_count=len(set(assay_ids)))
    class_map_dict = client.fetch_assay_class_map_by_assay_ids(
        assay_ids,
        list(class_map_fields),
        page_limit,
    )

    # Собрать все уникальные assay_class_id
    all_class_ids: set[str] = set()
    for mappings in class_map_dict.values():
        for mapping in mappings:
            class_id = mapping.get("assay_class_id")
            if class_id and not (isinstance(class_id, float) and pd.isna(class_id)):
                all_class_ids.add(str(class_id).strip())

    # Шаг 2: Получить ASSAY_CLASSIFICATION по assay_class_id
    classification_dict: dict[str, dict[str, Any]] = {}
    if all_class_ids:
        log.info("enrichment_fetching_assay_classifications", class_ids_count=len(all_class_ids))
        classification_dict = client.fetch_assay_classifications_by_class_ids(
            list(all_class_ids),
            list(classification_fields),
            page_limit,
        )

    # Шаг 3: Объединить данные и создать структуры для каждого assay
    df_assay = df_assay.copy()

    # Инициализируем колонки если их нет
    if "assay_classifications" not in df_assay.columns:
        df_assay["assay_classifications"] = pd.NA
    if "assay_class_id" not in df_assay.columns:
        df_assay["assay_class_id"] = pd.NA

    # Обработать каждую запись assay
    for idx, row in df_assay.iterrows():
        row_key: Any = idx
        assay_id = row.get("assay_chembl_id")
        if pd.isna(assay_id) or assay_id is None:
            continue
        assay_id_str = str(assay_id).strip()

        # Получить mappings для этого assay
        mappings = class_map_dict.get(assay_id_str, [])

        if not mappings:
            # Нет классификаций - оставляем NULL
            continue

        # Собрать данные классификаций
        classifications: list[dict[str, Any]] = []
        class_ids: list[str] = []

        for mapping in mappings:
            class_id = mapping.get("assay_class_id")
            if not class_id or (isinstance(class_id, float) and pd.isna(class_id)):
                continue

            class_id_str = str(class_id).strip()
            if not class_id_str:
                continue

            # Получить данные классификации
            classification_data = classification_dict.get(class_id_str)
            if classification_data:
                # Создать объединенную структуру
                class_record: dict[str, Any] = {
                    "assay_class_id": class_id_str,
                }
                # Добавить поля из classification_data
                for field in classification_fields:
                    if field != "assay_class_id":
                        class_record[field] = classification_data.get(field)
                classifications.append(class_record)
            else:
                # Если нет данных классификации, создаем минимальную запись
                class_record = {"assay_class_id": class_id_str}
                classifications.append(class_record)

            class_ids.append(class_id_str)

        # Сохранить результаты
        if classifications:
            serialized = json.dumps(classifications, ensure_ascii=False)
            class_id_joined = ";".join(class_ids)
            df_assay.at[row_key, "assay_classifications"] = serialized
            df_assay.at[row_key, "assay_class_id"] = class_id_joined

    log.info(
        "enrichment_classifications_complete",
        assays_with_classifications=len(df_assay[df_assay["assay_classifications"].notna()]),
    )
    return ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)


def enrich_with_assay_parameters(
    df_assay: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Обогатить DataFrame assay данными из ASSAY_PARAMETERS.

    Извлекает полный TRUV-набор полей (TYPE, RELATION, VALUE, UNITS, TEXT_VALUE),
    стандартизованные поля (standard_*), служебные поля (active) и опциональные
    поля нормализации (type_normalized, type_fixed).

    Parameters
    ----------
    df_assay:
        DataFrame с данными assay, должен содержать assay_chembl_id.
    client:
        ChemblClient для запросов к ChEMBL API.
    cfg:
        Конфигурация обогащения из config.chembl.assay.enrich.parameters.
        Должна содержать fields (список полей для извлечения), page_limit и active_only.

    Returns
    -------
    pd.DataFrame:
        Обогащенный DataFrame с добавленной/обновленной колонкой:
        - assay_parameters (string, nullable) - сериализованный JSON-массив параметров
          с полями: type, relation, value, units, text_value, standard_*,
          active, type_normalized, type_fixed (если присутствуют в дампе)

    Notes
    -----
    - Исходные значения сохраняются как есть, не копируются в standard_* автоматически
    - Опциональные поля (type_normalized, type_fixed) извлекаются только если
      присутствуют в дампе ChEMBL
    - Параметры фильтруются по active=1, если active_only=True в конфигурации
    """
    log = UnifiedLogger.get(__name__).bind(component="assay_enrichment")

    df_assay = _ensure_columns(df_assay, _PARAMETERS_COLUMNS)

    if df_assay.empty:
        log.debug("enrichment_skipped_empty_dataframe")
        return ASSAY_PARAMETERS_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Проверка наличия необходимых колонок
    required_cols = ["assay_chembl_id"]
    missing_cols = [col for col in required_cols if col not in df_assay.columns]
    if missing_cols:
        log.warning(
            "enrichment_skipped_missing_columns",
            missing_columns=missing_cols,
        )
        return ASSAY_PARAMETERS_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Собрать уникальные assay_chembl_id, dropna
    assay_ids: list[str] = []
    for _, row in df_assay.iterrows():
        assay_id = row.get("assay_chembl_id")

        # Пропускаем NaN/None значения
        if pd.isna(assay_id) or assay_id is None:
            continue

        # Преобразуем в строку
        assay_id_str = str(assay_id).strip()

        if assay_id_str:
            assay_ids.append(assay_id_str)

    if not assay_ids:
        log.debug("enrichment_skipped_no_valid_ids")
        return ASSAY_PARAMETERS_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Получить конфигурацию
    # Значения по умолчанию включают полный TRUV-набор и стандартизованные поля
    fields = cfg.get(
        "fields",
        [
            "assay_chembl_id",
            "type",
            "relation",
            "value",
            "units",
            "text_value",
            "standard_type",
            "standard_relation",
            "standard_value",
            "standard_units",
            "standard_text_value",
            "active",
        ],
    )
    page_limit = cfg.get("page_limit", 1000)
    active_only = cfg.get("active_only", True)

    # Получить ASSAY_PARAMETERS по assay_chembl_id
    log.info("enrichment_fetching_assay_parameters", ids_count=len(set(assay_ids)))
    parameters_dict = client.fetch_assay_parameters_by_assay_ids(
        assay_ids,
        list(fields),
        page_limit,
        active_only,
    )

    # Обработать каждую запись assay
    df_assay = df_assay.copy()

    # Инициализируем колонку если её нет
    if "assay_parameters" not in df_assay.columns:
        df_assay["assay_parameters"] = pd.NA

    # Обработать каждую запись assay
    for idx, row in df_assay.iterrows():
        assay_id = row.get("assay_chembl_id")
        if pd.isna(assay_id) or assay_id is None:
            continue
        assay_id_str = str(assay_id).strip()

        # Получить параметры для этого assay
        parameters = parameters_dict.get(assay_id_str, [])

        if not parameters:
            # Нет параметров - оставляем NULL
            continue

        # Создать список параметров с нужными полями
        params_list: list[dict[str, Any]] = []
        for param in parameters:
            param_record: dict[str, Any] = {}
            for field in fields:
                if field != "assay_chembl_id":
                    param_record[field] = param.get(field)
            params_list.append(param_record)

        # Сериализовать массив в JSON-строку
        if params_list:
            df_assay.at[idx, "assay_parameters"] = json.dumps(params_list, ensure_ascii=False)

    log.info(
        "enrichment_parameters_complete",
        assays_with_parameters=len(df_assay[df_assay["assay_parameters"].notna()]),
    )
    return ASSAY_PARAMETERS_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)
