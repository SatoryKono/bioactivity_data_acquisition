"""Enrichment functions for Activity pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from bioetl.clients import ChemblClient
from bioetl.core.logger import UnifiedLogger

__all__ = ["enrich_with_assay", "enrich_with_compound_record", "enrich_with_data_validity"]


def enrich_with_assay(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Обогатить DataFrame activity полями из /assay (ChEMBL v2).

    Требуемые входные колонки: assay_chembl_id.
    Добавляет:
      - assay_organism : pandas.StringDtype (nullable)
      - assay_tax_id   : pandas.Int64Dtype  (nullable)
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_enrichment")

    if df_act.empty:
        log.debug("enrichment_skipped_empty_dataframe")
        return df_act

    # 1) Валидация наличия ключа
    if "assay_chembl_id" not in df_act.columns:
        log.warning("enrichment_skipped_missing_columns", missing_columns=["assay_chembl_id"])
        return df_act

    # 2) Собрать уникальные валидные идентификаторы (без iterrows: быстрее и чище)
    assay_ids = (
        df_act["assay_chembl_id"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    assay_ids = assay_ids[assay_ids.ne("")].unique().tolist()

    # Гарантируем наличие выходных колонок даже при пустом наборе ID
    if not assay_ids:
        for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
            if col not in df_act.columns:
                df_act[col] = pd.Series(pd.NA, index=df_act.index, dtype=dtype)
        log.debug("enrichment_skipped_no_valid_ids")
        return df_act

    # 3) Конфигурация (явно фиксируем корректные имена полей ChEMBL /assay)
    #    NB: В ChEMBL JSON поле называется 'assay_organism', не 'organism'
    fields_cfg = cfg.get(
        "fields",
        ["assay_chembl_id", "assay_organism", "assay_tax_id"],
    )
    # Жёстко гарантируем, что критические поля присутствуют
    required_fields = {"assay_chembl_id", "assay_organism", "assay_tax_id"}
    fields = list(dict.fromkeys(list(fields_cfg) + list(required_fields)))

    page_limit = int(cfg.get("page_limit", 1000))

    # 4) Вызов клиента
    # Ожидается, что клиент:
    #  - проставит only=fields
    #  - корректно пройдёт пагинацию по page_meta (limit/offset/next/total_count)
    #  - вернёт dict: {assay_chembl_id: record_dict}
    log.info("enrichment_fetching_assays", ids_count=len(assay_ids))
    records_by_id: dict[str, dict[str, Any]] = client.fetch_assays_by_ids(
        ids=assay_ids,
        fields=fields,
        page_limit=page_limit,
    ) or {}

    # 5) Построить таблицу обогащения (только нужные выходные поля)
    if not records_by_id:
        for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
            if col not in df_act.columns:
                df_act[col] = pd.Series(pd.NA, index=df_act.index, dtype=dtype)
        log.debug("enrichment_no_records_found")
        return df_act

    enrichment_rows: list[dict[str, Any]] = []
    for assay_id, rec in records_by_id.items():
        enrichment_rows.append(
            {
                "assay_chembl_id": assay_id,
                "assay_organism": rec.get("assay_organism"),
                "assay_tax_id": rec.get("assay_tax_id"),
            }
        )

    df_enrich = pd.DataFrame(enrichment_rows)

    # 6) Левый джойн по ключу, порядок строк — как во входном df_act
    original_index = df_act.index
    df_merged = df_act.merge(
        df_enrich,
        on="assay_chembl_id",
        how="left",
        sort=False,
        suffixes=("", "_enrich"),
    ).reindex(original_index)

    # 7) Приведение типов (софт)
    if "assay_organism" not in df_merged.columns:
        df_merged["assay_organism"] = pd.NA
    if "assay_tax_id" not in df_merged.columns:
        df_merged["assay_tax_id"] = pd.NA

    df_merged["assay_organism"] = df_merged["assay_organism"].astype("string")

    # assay_tax_id может приходить строкой — приводим к Int64 с NA
    tax_id_numeric: pd.Series[Any] = pd.to_numeric(df_merged["assay_tax_id"], errors="coerce")  # type: ignore[reportUnknownMemberType]
    df_merged["assay_tax_id"] = tax_id_numeric.astype("Int64")

    log.info(
        "enrichment_completed",
        rows_enriched=int(df_merged.shape[0]),
        records_matched=int(len(records_by_id)),
    )
    return df_merged


def enrich_with_compound_record(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Обогатить DataFrame activity полями из compound_record.

    Parameters
    ----------
    df_act:
        DataFrame с данными activity, должен содержать molecule_chembl_id.
        Для строк с document_chembl_id используется путь через пары (molecule_chembl_id, document_chembl_id).
        Для строк без document_chembl_id используется fallback через record_id.
    client:
        ChemblClient для запросов к ChEMBL API.
    cfg:
        Конфигурация обогащения из config.chembl.activity.enrich.compound_record.

    Returns
    -------
    pd.DataFrame:
        Обогащенный DataFrame с добавленными колонками:
        - compound_name (nullable string) - из compound_record.compound_name
        - compound_key (nullable string) - из compound_record.compound_key
        - curated (nullable bool) - из compound_record.curated
        - removed (nullable bool) - всегда NULL (не извлекается из ChEMBL)
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_enrichment")

    if df_act.empty:
        log.debug("enrichment_skipped_empty_dataframe")
        return df_act

    # Проверка наличия необходимых колонок
    if "molecule_chembl_id" not in df_act.columns:
        log.warning(
            "enrichment_skipped_missing_columns",
            missing_columns=["molecule_chembl_id"],
        )
        # Добавим пустые колонки нужных типов
        for col, dtype in [
            ("compound_name", "string"),
            ("compound_key", "string"),
            ("curated", "boolean"),
            ("removed", "boolean"),
        ]:
            if col not in df_act.columns:
                df_act[col] = pd.Series(pd.NA, index=df_act.index, dtype=dtype)
        return df_act

    # 1) Сохранение порядка строк: добавить временный столбец _row_id
    df_act = df_act.copy()
    df_act["_row_id"] = np.arange(len(df_act))

    # 2) Разделить DataFrame на две части: с document_chembl_id и без него
    has_doc_id = "document_chembl_id" in df_act.columns
    if has_doc_id:
        mask_with_doc = df_act["document_chembl_id"].notna() & (
            df_act["document_chembl_id"].astype("string").str.strip() != ""
        )
        df_with_doc = df_act[mask_with_doc].copy()
        df_without_doc = df_act[~mask_with_doc].copy()
    else:
        df_with_doc = pd.DataFrame()
        df_without_doc = df_act.copy()

    # 3) Обогащение для строк с document_chembl_id через пары
    enrichment_by_pairs: pd.DataFrame | None = None
    if not df_with_doc.empty:
        enrichment_by_pairs = _enrich_by_pairs(df_with_doc, client, cfg, log)

    # 4) Определить строки, которые нуждаются в fallback через record_id:
    #    - строки без document_chembl_id
    #    - строки с document_chembl_id, но где обогащение через пары не дало результата
    df_need_fallback = df_without_doc.copy()
    if enrichment_by_pairs is not None and not enrichment_by_pairs.empty:
        # Проверить строки из обогащения через пары, где compound_name/compound_key пустые
        mask_empty = (
            enrichment_by_pairs["compound_name"].isna()
            | (enrichment_by_pairs["compound_name"].astype("string").str.strip() == "")
        ) & (
            enrichment_by_pairs["compound_key"].isna()
            | (enrichment_by_pairs["compound_key"].astype("string").str.strip() == "")
        )
        rows_need_fallback = enrichment_by_pairs[mask_empty].copy()
        if not rows_need_fallback.empty and "record_id" in rows_need_fallback.columns:
            # Добавить эти строки к df_need_fallback для fallback
            df_need_fallback = pd.concat([df_need_fallback, rows_need_fallback], ignore_index=True)

    # 5) Обогащение через record_id (fallback) для строк, которые в этом нуждаются
    enrichment_by_record_id: pd.DataFrame | None = None
    if not df_need_fallback.empty and "record_id" in df_need_fallback.columns:
        # Убрать дубликаты по _row_id, если они есть
        df_need_fallback = df_need_fallback.drop_duplicates(subset=["_row_id"], keep="first")
        enrichment_by_record_id = _enrich_by_record_id(df_need_fallback, client, cfg, log)

    # 6) Объединить результаты с приоритетом: данные из пар > данные из fallback
    # Начинаем с исходного DataFrame и применяем обогащения последовательно
    df_result = df_act.copy()

    # Применить обогащение через пары для строк с document_chembl_id
    if enrichment_by_pairs is not None and not enrichment_by_pairs.empty:
        # Создать словарь данных из обогащения через пары по _row_id
        pairs_dict: dict[int, dict[str, Any]] = {}
        for _, row in enrichment_by_pairs.iterrows():
            row_id_raw = row.get("_row_id")
            if not pd.isna(row_id_raw):
                try:
                    # Безопасное преобразование в int
                    if isinstance(row_id_raw, (int, float)):
                        row_id = int(row_id_raw)
                    else:
                        row_id = int(str(row_id_raw))
                    pairs_dict[row_id] = {
                        "compound_name": row.get("compound_name"),
                        "compound_key": row.get("compound_key"),
                        "curated": row.get("curated"),
                    }
                except (ValueError, TypeError):
                    continue

        # Применить данные из обогащения через пары
        if "_row_id" in df_result.columns:
            for idx in df_result.index:
                row_id_raw = df_result.loc[idx, "_row_id"]
                if not pd.isna(row_id_raw):
                    try:
                        # Безопасное преобразование в int
                        if isinstance(row_id_raw, (int, float)):
                            row_id = int(row_id_raw)
                        else:
                            row_id = int(str(row_id_raw))
                        if row_id in pairs_dict:
                            pairs_data = pairs_dict[row_id]
                            if pairs_data.get("compound_name") is not None:
                                df_result.loc[idx, "compound_name"] = pairs_data["compound_name"]
                            if pairs_data.get("compound_key") is not None:
                                df_result.loc[idx, "compound_key"] = pairs_data["compound_key"]
                            if pairs_data.get("curated") is not None:
                                df_result.loc[idx, "curated"] = pairs_data["curated"]
                    except (ValueError, TypeError):
                        continue

    # Восстановить исходный порядок по _row_id
    if "_row_id" in df_result.columns:
        df_result = df_result.sort_values("_row_id").reset_index(drop=True)

    # Применить fallback данные только для строк, где compound_name/compound_key пустые
    if enrichment_by_record_id is not None and not enrichment_by_record_id.empty:
        # Создать словарь fallback данных по _row_id
        fallback_dict: dict[int, dict[str, Any]] = {}
        for _, row in enrichment_by_record_id.iterrows():
            row_id_raw = row.get("_row_id")
            if not pd.isna(row_id_raw):
                try:
                    # Безопасное преобразование в int
                    if isinstance(row_id_raw, (int, float)):
                        row_id = int(row_id_raw)
                    else:
                        row_id = int(str(row_id_raw))
                    fallback_dict[row_id] = {
                        "compound_name": row.get("compound_name"),
                        "compound_key": row.get("compound_key"),
                    }
                except (ValueError, TypeError):
                    continue

        # Применить fallback данные только для строк, где compound_name/compound_key пустые
        if "_row_id" in df_result.columns:
            for idx in df_result.index:
                row_id_raw = df_result.loc[idx, "_row_id"]
                if not pd.isna(row_id_raw):
                    try:
                        # Безопасное преобразование в int
                        if isinstance(row_id_raw, (int, float)):
                            row_id = int(row_id_raw)
                        else:
                            row_id = int(str(row_id_raw))
                        if row_id in fallback_dict:
                            # Проверить, нужно ли применять fallback (если compound_name/compound_key пустые)
                            compound_name = df_result.loc[idx, "compound_name"] if "compound_name" in df_result.columns else pd.NA
                            compound_key = df_result.loc[idx, "compound_key"] if "compound_key" in df_result.columns else pd.NA

                            name_empty = pd.isna(compound_name) or (str(compound_name).strip() == "")
                            key_empty = pd.isna(compound_key) or (str(compound_key).strip() == "")

                            if name_empty or key_empty:
                                fallback_data = fallback_dict[row_id]
                                if name_empty and fallback_data.get("compound_name") is not None:
                                    df_result.loc[idx, "compound_name"] = fallback_data["compound_name"]
                                if key_empty and fallback_data.get("compound_key") is not None:
                                    df_result.loc[idx, "compound_key"] = fallback_data["compound_key"]
                    except (ValueError, TypeError):
                        continue

    # 6) Убедиться, что все новые колонки присутствуют (заполнить NA для отсутствующих)
    for col in ["compound_name", "compound_key", "curated", "removed"]:
        if col not in df_result.columns:
            df_result[col] = pd.NA

    # 7) removed всегда <NA> на этом этапе
    df_result["removed"] = pd.NA

    # 8) Удалить временную колонку _row_id
    if "_row_id" in df_result.columns:
        df_result = df_result.drop(columns=["_row_id"])

    # 9) Надёжное приведение булевых для curated
    if "curated" in df_result.columns:
        # Нормализовать возможные представления перед приведением к boolean
        df_result["curated"] = df_result["curated"].replace({  # type: ignore[reportUnknownMemberType]
            1: True,
            0: False,
            "1": True,
            "0": False,
        })
        df_result["curated"] = df_result["curated"].astype("boolean")

    # 10) Нормализовать типы для строковых полей
    df_result["compound_name"] = df_result["compound_name"].astype("string")
    df_result["compound_key"] = df_result["compound_key"].astype("string")
    df_result["removed"] = df_result["removed"].astype("boolean")

    log.info(
        "enrichment_completed",
        rows_enriched=df_result.shape[0],
        rows_with_doc_id=len(df_with_doc) if not df_with_doc.empty else 0,
        rows_without_doc_id=len(df_without_doc) if not df_without_doc.empty else 0,
    )
    return df_result


def _enrich_by_pairs(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
    log: Any,
) -> pd.DataFrame:
    """Обогатить DataFrame activity через пары (molecule_chembl_id, document_chembl_id)."""
    # Нормализация ключей до сборки пар: upper, strip
    pairs_df = (
        df_act[["molecule_chembl_id", "document_chembl_id"]]
        .astype("string")
        .apply(lambda s: s.str.strip().str.upper(), axis=0)  # type: ignore[reportUnknownArgumentType,reportUnknownLambdaType,reportUnknownMemberType]
        .dropna()  # type: ignore[reportUnknownMemberType]
        .drop_duplicates()  # type: ignore[reportUnknownMemberType]
    )
    pairs: set[tuple[str, str]] = set(map(tuple, pairs_df.to_numpy()))

    if not pairs:
        log.debug("enrichment_by_pairs_skipped_no_valid_pairs")
        return df_act

    # Поля запроса к клиенту — плоские имена полей, которые возвращает ChEMBL API
    fields = cfg.get(
        "fields",
        [
            "molecule_chembl_id",
            "document_chembl_id",
            "compound_name",
            "compound_key",
            "curated",
        ],
    )
    page_limit = cfg.get("page_limit", 1000)

    # Обернуть вызов клиента в try/except — не роняем пайплайн
    log.info("enrichment_fetching_compound_records_by_pairs", pairs_count=len(pairs))
    compound_records_dict: dict[tuple[str, str], dict[str, Any]] = {}
    try:
        compound_records_dict = (
            client.fetch_compound_records_by_pairs(
                pairs=pairs,
                fields=list(fields),
                page_limit=page_limit,
            )
            or {}
        )
    except Exception as exc:
        log.warning(
            "enrichment_fetch_error_by_pairs",
            pairs_count=len(pairs),
            error=str(exc),
            exc_info=True,
        )
        return df_act

    # Маппинг результата клиента
    enrichment_data: list[dict[str, Any]] = []
    pairs_found = 0
    pairs_not_found = 0
    for pair in pairs:
        compound_record: dict[str, Any] | None = compound_records_dict.get(pair)
        if compound_record:
            pairs_found += 1
            # Проверить, что поля действительно присутствуют в ответе
            compound_name = compound_record.get("compound_name")
            compound_key = compound_record.get("compound_key")
            curated = compound_record.get("curated")

            enrichment_data.append({
                "molecule_chembl_id": pair[0],
                "document_chembl_id": pair[1],
                "compound_name": compound_name if compound_name is not None else None,
                "compound_key": compound_key if compound_key is not None else None,
                "curated": curated if curated is not None else None,
                "removed": None,
            })
        else:
            pairs_not_found += 1
            enrichment_data.append({
                "molecule_chembl_id": pair[0],
                "document_chembl_id": pair[1],
                "compound_name": None,
                "compound_key": None,
                "curated": None,
                "removed": None,
            })

    # Логирование результатов
    log.info(
        "enrichment_by_pairs_complete",
        pairs_requested=len(pairs),
        pairs_found=pairs_found,
        pairs_not_found=pairs_not_found,
        records_returned=len(compound_records_dict),
    )

    if pairs_not_found > 0:
        log.warning(
            "enrichment_by_pairs_some_pairs_not_found",
            pairs_not_found=pairs_not_found,
            pairs_total=len(pairs),
            hint="Проверьте, что пары (molecule_chembl_id, document_chembl_id) существуют в ChEMBL API",
        )

    if not enrichment_data:
        log.debug("enrichment_by_pairs_no_records_found")
        return df_act

    df_enrich = pd.DataFrame(enrichment_data)

    # Нормализовать ключи в df_enrich для join (upper, strip)
    df_enrich["molecule_chembl_id"] = (
        df_enrich["molecule_chembl_id"].astype("string").str.strip().str.upper()
    )
    df_enrich["document_chembl_id"] = (
        df_enrich["document_chembl_id"].astype("string").str.strip().str.upper()
    )

    # Нормализовать ключи в df_act для join (upper, strip)
    df_act["molecule_chembl_id_normalized"] = (
        df_act["molecule_chembl_id"].astype("string").str.strip().str.upper()
    )
    df_act["document_chembl_id_normalized"] = (
        df_act["document_chembl_id"].astype("string").str.strip().str.upper()
    )

    # Left-join обратно к df_act на нормализованные ключи
    df_result = df_act.merge(
        df_enrich,
        left_on=["molecule_chembl_id_normalized", "document_chembl_id_normalized"],
        right_on=["molecule_chembl_id", "document_chembl_id"],
        how="left",
        suffixes=("", "_enrich"),
    )

    # Удалить временные нормализованные колонки
    df_result = df_result.drop(
        columns=["molecule_chembl_id_normalized", "document_chembl_id_normalized"]
    )

    # Коалесценс после merge: *_enrich → базовые колонки
    for col in ["compound_name", "compound_key", "curated"]:
        if f"{col}_enrich" in df_result.columns:
            if col not in df_result.columns:
                df_result[col] = df_result[f"{col}_enrich"]
            else:
                df_result[col] = df_result[col].where(
                    df_result[col].notna(),
                    df_result[f"{col}_enrich"],
                )
            df_result = df_result.drop(columns=[f"{col}_enrich"])

    return df_result


def _enrich_by_record_id(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
    log: Any,
) -> pd.DataFrame:
    """Обогатить DataFrame activity через record_id (fallback для строк без document_chembl_id)."""
    # Собрать уникальные record_id
    record_ids: set[str] = set()
    for _, row in df_act.iterrows():
        rec = row.get("record_id")
        if rec is not None and not pd.isna(rec):
            rec_s = str(rec).strip()
            if rec_s:
                record_ids.add(rec_s)

    if not record_ids:
        log.debug("enrichment_by_record_id_skipped_no_valid_ids")
        return df_act

    # Поля запроса к клиенту — плоские имена полей
    fields = ["record_id", "compound_name", "compound_key"]
    page_limit = cfg.get("page_limit", 1000)
    batch_size = int(cfg.get("batch_size", 100)) or 100

    # Получить compound_record по record_id
    log.info("enrichment_fetching_compound_records_by_record_id", record_ids_count=len(record_ids))
    compound_records_dict: dict[str, dict[str, Any]] = {}
    try:
        unique_ids = list(record_ids)
        all_records: list[dict[str, Any]] = []

        # Обработка батчами по фильтру record_id__in
        for i in range(0, len(unique_ids), batch_size):
            chunk = unique_ids[i : i + batch_size]
            params: dict[str, Any] = {
                "record_id__in": ",".join(chunk),
                "limit": page_limit,
                "only": ",".join(fields),
                "order_by": "record_id",
            }

            try:
                for record in client.paginate(
                    "/compound_record.json",
                    params=params,
                    page_size=page_limit,
                    items_key="compound_records",
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                log.warning(
                    "enrichment_fetch_error_by_record_id",
                    chunk_size=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        # Построить словарь по record_id
        for record in all_records:
            rid_raw = record.get("record_id")
            if rid_raw is None:
                continue
            rid_str = str(rid_raw).strip()
            if rid_str and rid_str not in compound_records_dict:
                compound_records_dict[rid_str] = {
                    "record_id": rid_str,
                    "compound_key": record.get("compound_key"),
                    "compound_name": record.get("compound_name"),
                }
    except Exception as exc:
        log.warning(
            "enrichment_fetch_error_by_record_id",
            record_ids_count=len(record_ids),
            error=str(exc),
            exc_info=True,
        )
        return df_act

    if not compound_records_dict:
        log.debug("enrichment_by_record_id_no_records_found")
        return df_act

    # Создать DataFrame для join
    compound_data: list[dict[str, Any]] = []
    for record_id, record in compound_records_dict.items():
        compound_data.append({
            "record_id": record_id,
            "compound_key": record.get("compound_key"),
            "compound_name": record.get("compound_name"),
        })

    df_compound = pd.DataFrame(compound_data) if compound_data else pd.DataFrame(
        columns=["record_id", "compound_key", "compound_name"]
    )

    # Нормализовать record_id в df_act для join
    df_act_normalized = df_act.copy()
    if "record_id" in df_act_normalized.columns:
        mask_na = df_act_normalized["record_id"].isna()
        df_act_normalized["record_id"] = df_act_normalized["record_id"].astype(str)
        df_act_normalized.loc[df_act_normalized["record_id"] == "nan", "record_id"] = pd.NA
        df_act_normalized.loc[mask_na, "record_id"] = pd.NA
        if "record_id" in df_compound.columns and not df_compound.empty:
            df_compound["record_id"] = df_compound["record_id"].astype(str)
            df_compound.loc[df_compound["record_id"] == "nan", "record_id"] = pd.NA

    # Left-join по record_id
    df_result = df_act_normalized.merge(
        df_compound,
        on=["record_id"],
        how="left",
        suffixes=("", "_compound"),
    )

    # Коалесценс после merge: *_compound → базовые колонки
    for col in ["compound_name", "compound_key"]:
        if f"{col}_compound" in df_result.columns:
            if col not in df_result.columns:
                df_result[col] = df_result[f"{col}_compound"]
            else:
                df_result[col] = df_result[col].where(
                    df_result[col].notna(),
                    df_result[f"{col}_compound"],
                )
            df_result = df_result.drop(columns=[f"{col}_compound"])

    # Добавить curated и removed (всегда None для этого пути)
    if "curated" not in df_result.columns:
        df_result["curated"] = pd.NA
    if "removed" not in df_result.columns:
        df_result["removed"] = pd.NA

    return df_result


def enrich_with_data_validity(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Обогатить DataFrame activity полями из data_validity_lookup.

    Parameters
    ----------
    df_act:
        DataFrame с данными activity, должен содержать data_validity_comment.
    client:
        ChemblClient для запросов к ChEMBL API.
    cfg:
        Конфигурация обогащения из config.chembl.activity.enrich.data_validity.

    Returns
    -------
    pd.DataFrame:
        Обогащенный DataFrame с добавленной колонкой:
        - data_validity_description (nullable string) - из DATA_VALIDITY_LOOKUP.DESCRIPTION
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_enrichment")

    if df_act.empty:
        log.debug("enrichment_skipped_empty_dataframe")
        return df_act

    # Проверка наличия необходимых колонок
    required_cols = ["data_validity_comment"]
    missing_cols = [col for col in required_cols if col not in df_act.columns]
    if missing_cols:
        log.warning(
            "enrichment_skipped_missing_columns",
            missing_columns=missing_cols,
        )
        # Добавим пустую колонку data_validity_description
        if "data_validity_description" not in df_act.columns:
            df_act["data_validity_description"] = pd.NA
        return df_act

    # Собрать уникальные data_validity_comment, dropna
    validity_comments: list[str] = []
    for _, row in df_act.iterrows():
        comment = row.get("data_validity_comment")

        # Пропускаем NaN/None значения
        if pd.isna(comment) or comment is None:
            continue

        # Преобразуем в строку
        comment_str = str(comment).strip()

        if comment_str:
            validity_comments.append(comment_str)

    if not validity_comments:
        log.debug("enrichment_skipped_no_valid_comments")
        # Добавим пустую колонку
        if "data_validity_description" not in df_act.columns:
            df_act["data_validity_description"] = pd.NA
        return df_act

    # Получить конфигурацию
    fields = cfg.get("fields", ["data_validity_comment", "description"])
    page_limit = cfg.get("page_limit", 1000)

    # Вызвать client.fetch_data_validity_lookup
    log.info("enrichment_fetching_data_validity", comments_count=len(set(validity_comments)))
    records_dict = client.fetch_data_validity_lookup(
        comments=validity_comments,
        fields=list(fields),
        page_limit=page_limit,
    )

    # Создать DataFrame для join
    enrichment_data: list[dict[str, Any]] = []
    for comment in set(validity_comments):
        record = records_dict.get(comment)
        if record:
            enrichment_data.append({
                "data_validity_comment": comment,
                "data_validity_description": record.get("description"),
            })
        else:
            # Если запись не найдена, оставляем description как NULL
            enrichment_data.append({
                "data_validity_comment": comment,
                "data_validity_description": None,
            })

    if not enrichment_data:
        log.debug("enrichment_no_records_found")
        # Добавим пустую колонку
        if "data_validity_description" not in df_act.columns:
            df_act["data_validity_description"] = pd.NA
        return df_act

    df_enrich = pd.DataFrame(enrichment_data)

    # Left-join обратно к df_act на data_validity_comment
    # Сохраняем исходный порядок строк через индекс
    original_index = df_act.index.copy()
    df_result = df_act.merge(
        df_enrich,
        on=["data_validity_comment"],
        how="left",
        suffixes=("", "_enrich"),
    )

    # Убедиться, что колонка присутствует (заполнить NA для отсутствующих)
    if "data_validity_description" not in df_result.columns:
        df_result["data_validity_description"] = pd.NA

    # Восстановить исходный порядок
    df_result = df_result.reindex(original_index)

    # Нормализовать типы
    df_result["data_validity_description"] = df_result["data_validity_description"].astype("string")

    log.info(
        "enrichment_completed",
        rows_enriched=df_result.shape[0],
        records_matched=len(records_dict),
    )
    return df_result

