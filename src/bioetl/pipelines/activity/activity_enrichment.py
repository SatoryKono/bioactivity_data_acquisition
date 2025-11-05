"""Enrichment functions for Activity pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import pandas as pd

from bioetl.clients.chembl import ChemblClient
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
    records_by_id = client.fetch_assays_by_ids(
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
    tax_id_numeric: pd.Series[Any] = pd.to_numeric(df_merged["assay_tax_id"], errors="coerce")  # type: ignore[arg-type]
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
    """Обогатить DataFrame activity полями из molecule_dictionary и compound_structures.

    Parameters
    ----------
    df_act:
        DataFrame с данными activity, должен содержать molecule_chembl_id.
    client:
        ChemblClient для запросов к ChEMBL API.
    cfg:
        Конфигурация обогащения из config.chembl.activity.enrich.compound_record.

    Returns
    -------
    pd.DataFrame:
        Обогащенный DataFrame с добавленными колонками:
        - compound_name (nullable string) - из MOLECULE_DICTIONARY.PREF_NAME (fallback: CHEMBL_ID)
        - compound_key (nullable string) - из COMPOUND_STRUCTURES.STANDARD_INCHI_KEY
        - curated (nullable bool) - из ACTIVITIES.CURATED_BY (обрабатывается в _extract_nested_fields)
        - removed (nullable bool) - всегда NULL (не извлекается из ChEMBL)
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_enrichment")

    if df_act.empty:
        log.debug("enrichment_skipped_empty_dataframe")
        return df_act

    # Проверка наличия необходимых колонок
    required_cols = ["molecule_chembl_id"]
    missing_cols = [col for col in required_cols if col not in df_act.columns]
    if missing_cols:
        log.warning(
            "enrichment_skipped_missing_columns",
            missing_columns=missing_cols,
        )
        return df_act

    # Собрать уникальные molecule_chembl_id, dropna
    molecule_ids: list[str] = []
    for _, row in df_act.iterrows():
        mol_id = row.get("molecule_chembl_id")

        # Пропускаем NaN/None значения
        if pd.isna(mol_id) or mol_id is None:
            continue

        # Преобразуем в строку
        mol_id_str = str(mol_id).strip()

        if mol_id_str:
            molecule_ids.append(mol_id_str)

    if not molecule_ids:
        log.debug("enrichment_skipped_no_valid_ids")
        # Добавим пустые колонки
        for col in ["compound_name", "compound_key", "removed"]:
            if col not in df_act.columns:
                df_act[col] = pd.NA
        return df_act

    # Получить конфигурацию
    fields = cfg.get("fields", ["molecule_chembl_id", "pref_name", "molecule_structures"])
    page_limit = cfg.get("page_limit", 1000)

    # Вызвать client.fetch_molecules_by_ids для получения molecule_dictionary и compound_structures
    log.info("enrichment_fetching_molecules", ids_count=len(set(molecule_ids)))
    molecules_dict = client.fetch_molecules_by_ids(
        ids=molecule_ids,
        fields=list(fields),
        page_limit=page_limit,
    )

    # Создать DataFrame для join
    enrichment_data: list[dict[str, Any]] = []
    for mol_id in set(molecule_ids):
        molecule_record = molecules_dict.get(mol_id)
        if molecule_record:
            # compound_name: MOLECULE_DICTIONARY.PREF_NAME (fallback: MOLECULE_DICTIONARY.CHEMBL_ID)
            compound_name = molecule_record.get("pref_name")
            if not compound_name or pd.isna(compound_name):
                compound_name = molecule_record.get("molecule_chembl_id")

            # compound_key: COMPOUND_STRUCTURES.STANDARD_INCHI_KEY
            compound_key: str | None = None
            molecule_structures: Any = molecule_record.get("molecule_structures")
            if molecule_structures:
                # molecule_structures может быть списком или словарем
                if isinstance(molecule_structures, list) and len(molecule_structures) > 0:  # type: ignore[arg-type]
                    # Берем первую структуру с standard_inchi_key
                    for struct in molecule_structures:  # type: ignore[assignment]
                        if isinstance(struct, dict):
                            struct_dict: Mapping[str, Any] = cast(Mapping[str, Any], struct)
                            standard_inchi_key: Any = struct_dict.get("standard_inchi_key")
                            if standard_inchi_key:
                                compound_key = str(standard_inchi_key)
                                break
                elif isinstance(molecule_structures, dict):
                    structures_dict: Mapping[str, Any] = cast(Mapping[str, Any], molecule_structures)
                    standard_inchi_key_raw: Any = structures_dict.get("standard_inchi_key")
                    if standard_inchi_key_raw:
                        compound_key = str(standard_inchi_key_raw)

            enrichment_data.append({
                "molecule_chembl_id": mol_id,
                "compound_name": compound_name,
                "compound_key": compound_key,
            })
        else:
            # Если molecule не найден, используем fallback для compound_name
            enrichment_data.append({
                "molecule_chembl_id": mol_id,
                "compound_name": mol_id,  # fallback: CHEMBL_ID
                "compound_key": None,
            })

    if not enrichment_data:
        log.debug("enrichment_no_records_found")
        # Добавим пустые колонки
        for col in ["compound_name", "compound_key", "removed"]:
            if col not in df_act.columns:
                df_act[col] = pd.NA
        return df_act

    df_enrich = pd.DataFrame(enrichment_data)

    # Left-join обратно к df_act на molecule_chembl_id
    # Сохраняем исходный порядок строк через индекс
    original_index = df_act.index.copy()
    df_result = df_act.merge(
        df_enrich,
        on=["molecule_chembl_id"],
        how="left",
        suffixes=("", "_enrich"),
    )

    # Убедиться, что все новые колонки присутствуют (заполнить NA для отсутствующих)
    for col in ["compound_name", "compound_key", "removed"]:
        if col not in df_result.columns:
            df_result[col] = pd.NA

    # removed всегда NULL на стадии извлечения (не извлекается из ChEMBL)
    if "removed" in df_result.columns:
        df_result["removed"] = pd.NA

    # Восстановить исходный порядок
    df_result = df_result.reindex(original_index)

    # Нормализовать типы
    df_result["compound_name"] = df_result["compound_name"].astype("string")
    df_result["compound_key"] = df_result["compound_key"].astype("string")
    if "removed" in df_result.columns:
        df_result["removed"] = df_result["removed"].astype("boolean")

    log.info(
        "enrichment_completed",
        rows_enriched=df_result.shape[0],
        records_matched=len(molecules_dict),
    )
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

