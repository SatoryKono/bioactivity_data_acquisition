"""Enrichment functions for Activity pipeline."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd

from bioetl.clients.chembl import ChemblClient
from bioetl.core.logger import UnifiedLogger

__all__ = ["enrich_with_assay", "enrich_with_compound_record"]


def enrich_with_assay(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Обогатить DataFrame activity полями из assay.

    Parameters
    ----------
    df_act:
        DataFrame с данными activity, должен содержать assay_chembl_id.
    client:
        ChemblClient для запросов к ChEMBL API.
    cfg:
        Конфигурация обогащения из config.chembl.activity.enrich.assay.

    Returns
    -------
    pd.DataFrame:
        Обогащенный DataFrame с добавленными колонками:
        - assay_organism (nullable string)
        - assay_tax_id (nullable Int64)
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_enrichment")

    if df_act.empty:
        log.debug("enrichment_skipped_empty_dataframe")
        return df_act

    # Проверка наличия необходимых колонок
    required_cols = ["assay_chembl_id"]
    missing_cols = [col for col in required_cols if col not in df_act.columns]
    if missing_cols:
        log.warning(
            "enrichment_skipped_missing_columns",
            missing_columns=missing_cols,
        )
        return df_act

    # Собрать уникальные assay_chembl_id, dropna
    assay_ids: list[str] = []
    for _, row in df_act.iterrows():
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
        # Добавим пустые колонки
        for col in ["assay_organism", "assay_tax_id"]:
            if col not in df_act.columns:
                df_act[col] = pd.NA
        return df_act

    # Получить конфигурацию
    fields = cfg.get("fields", ["assay_chembl_id", "assay_organism", "assay_tax_id"])
    page_limit = cfg.get("page_limit", 1000)

    # Вызвать client.fetch_assays_by_ids
    log.info("enrichment_fetching_assays", ids_count=len(set(assay_ids)))
    records_dict = client.fetch_assays_by_ids(
        ids=assay_ids,
        fields=list(fields),
        page_limit=page_limit,
    )

    # Создать DataFrame для join
    enrichment_data: list[dict[str, Any]] = []
    for assay_id, record in records_dict.items():
        enrichment_data.append({
            "assay_chembl_id": assay_id,
            "assay_organism": record.get("assay_organism"),
            "assay_tax_id": record.get("assay_tax_id"),
        })

    if not enrichment_data:
        log.debug("enrichment_no_records_found")
        # Добавим пустые колонки
        for col in ["assay_organism", "assay_tax_id"]:
            if col not in df_act.columns:
                df_act[col] = pd.NA
        return df_act

    df_enrich = pd.DataFrame(enrichment_data)

    # Left-join обратно к df_act на assay_chembl_id
    # Сохраняем исходный порядок строк через индекс
    original_index = df_act.index.copy()
    df_result = df_act.merge(
        df_enrich,
        on=["assay_chembl_id"],
        how="left",
        suffixes=("", "_enrich"),
    )

    # Убедиться, что все новые колонки присутствуют (заполнить NA для отсутствующих)
    for col in ["assay_organism", "assay_tax_id"]:
        if col not in df_result.columns:
            df_result[col] = pd.NA

    # Восстановить исходный порядок
    df_result = df_result.reindex(original_index)

    # Нормализовать типы
    df_result["assay_organism"] = df_result["assay_organism"].astype("string")
    # assay_tax_id может быть строкой в API, но в схеме activity это Int64
    # Преобразуем в Int64, если возможно
    if "assay_tax_id" in df_result.columns:
        df_result["assay_tax_id"] = pd.to_numeric(df_result["assay_tax_id"], errors="coerce").astype("Int64")

    log.info(
        "enrichment_completed",
        rows_enriched=df_result.shape[0],
        records_matched=len(records_dict),
    )
    return df_result


def enrich_with_compound_record(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Обогатить DataFrame activity полями из compound_record.

    Parameters
    ----------
    df_act:
        DataFrame с данными activity, должен содержать molecule_chembl_id и document_chembl_id.
    client:
        ChemblClient для запросов к ChEMBL API.
    cfg:
        Конфигурация обогащения из config.chembl.activity.enrich.compound_record.

    Returns
    -------
    pd.DataFrame:
        Обогащенный DataFrame с добавленными колонками:
        - compound_name (nullable string)
        - compound_key (nullable string)
        - curated (nullable bool)
        - removed (nullable bool)
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_enrichment")

    if df_act.empty:
        log.debug("enrichment_skipped_empty_dataframe")
        return df_act

    # Проверка наличия необходимых колонок
    required_cols = ["molecule_chembl_id", "document_chembl_id"]
    missing_cols = [col for col in required_cols if col not in df_act.columns]
    if missing_cols:
        log.warning(
            "enrichment_skipped_missing_columns",
            missing_columns=missing_cols,
        )
        return df_act

    # Собрать уникальные пары (molecule_chembl_id, document_chembl_id), dropna
    pairs: list[tuple[str, str]] = []
    for _, row in df_act.iterrows():
        mol_id = row.get("molecule_chembl_id")
        doc_id = row.get("document_chembl_id")

        # Пропускаем NaN/None значения
        if pd.isna(mol_id) or pd.isna(doc_id):
            continue
        if mol_id is None or doc_id is None:
            continue

        # Преобразуем в строки
        mol_id_str = str(mol_id).strip()
        doc_id_str = str(doc_id).strip()

        if mol_id_str and doc_id_str:
            pairs.append((mol_id_str, doc_id_str))

    if not pairs:
        log.debug("enrichment_skipped_no_valid_pairs")
        # Добавим пустые колонки
        for col in ["compound_name", "compound_key", "curated", "removed"]:
            if col not in df_act.columns:
                df_act[col] = pd.NA
        return df_act

    # Получить конфигурацию
    fields = cfg.get("fields", ["compound_name", "compound_key", "curated", "removed", "molecule_chembl_id", "document_chembl_id"])
    page_limit = cfg.get("page_limit", 1000)

    # Вызвать client.fetch_compound_records_by_pairs
    # Дедупликация уже происходит внутри метода клиента
    log.info("enrichment_fetching_records", pairs_count=len(set(pairs)))
    records_dict = client.fetch_compound_records_by_pairs(
        pairs=pairs,
        fields=list(fields),
        page_limit=page_limit,
    )

    # Создать DataFrame для join
    enrichment_data: list[dict[str, Any]] = []
    for (mol_id, doc_id), record in records_dict.items():
        enrichment_data.append({
            "molecule_chembl_id": mol_id,
            "document_chembl_id": doc_id,
            "compound_name": record.get("compound_name"),
            "compound_key": record.get("compound_key"),
            "curated": _normalize_bool(record.get("curated")),
            "removed": _normalize_bool(record.get("removed")),
        })

    if not enrichment_data:
        log.debug("enrichment_no_records_found")
        # Добавим пустые колонки
        for col in ["compound_name", "compound_key", "curated", "removed"]:
            if col not in df_act.columns:
                df_act[col] = pd.NA
        return df_act

    df_enrich = pd.DataFrame(enrichment_data)

    # Left-join обратно к df_act на (molecule_chembl_id, document_chembl_id)
    # Сохраняем исходный порядок строк через индекс
    original_index = df_act.index.copy()
    df_result = df_act.merge(
        df_enrich,
        on=["molecule_chembl_id", "document_chembl_id"],
        how="left",
        suffixes=("", "_enrich"),
    )

    # Убедиться, что все новые колонки присутствуют (заполнить NA для отсутствующих)
    for col in ["compound_name", "compound_key", "curated", "removed"]:
        if col not in df_result.columns:
            df_result[col] = pd.NA

    # Восстановить исходный порядок
    df_result = df_result.reindex(original_index)

    # Нормализовать типы
    df_result["compound_name"] = df_result["compound_name"].astype("string")
    df_result["compound_key"] = df_result["compound_key"].astype("string")
    df_result["curated"] = df_result["curated"].astype("boolean")
    df_result["removed"] = df_result["removed"].astype("boolean")

    log.info(
        "enrichment_completed",
        rows_enriched=df_result.shape[0],
        records_matched=len(records_dict),
    )
    return df_result


def _normalize_bool(value: Any) -> bool | None:
    """Нормализовать значение к bool с поддержкой None/NA.

    Parameters
    ----------
    value:
        Значение для нормализации.

    Returns
    -------
    bool | None:
        Нормализованное булево значение или None.
    """
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return bool(value) and value != 0
    if isinstance(value, str):
        normalized = value.lower().strip()
        if normalized in ("true", "1", "yes", "on"):
            return True
        if normalized in ("false", "0", "no", "off", ""):
            return False
        return None
    return bool(value) if value else None

