"""Join activity with compound_record and molecule data."""

from __future__ import annotations

import math
import numbers
from collections.abc import Mapping, Sequence
from typing import Any, cast

import pandas as pd

from bioetl.clients import ChemblActivityClient, ChemblClient
from bioetl.core.logger import UnifiedLogger

__all__ = ["join_activity_with_molecule"]


def _normalize_chembl_id(value: Any) -> str:
    """Нормализовать ChEMBL ID: преобразовать в строку, обрезать пробелы.

    Args:
        value: Любое значение (может быть None, NaN, str, int и т.д.)

    Returns:
        Нормализованная строка или пустая строка, если значение невалидно.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    value_str = str(value).strip()
    return value_str if value_str else ""


def _canonical_record_id(value: Any) -> str:
    """Преобразовать record_id к каноническому строковому представлению."""

    if value is None:
        return ""
    if isinstance(value, numbers.Integral):
        return str(int(value))
    if isinstance(value, float):
        if pd.isna(value) or not math.isfinite(value):
            return ""
        value_float: float = float(value)
        value_int = math.trunc(value_float)
        if value_float == value_int:
            return str(value_int)
        return format(value_float, "g")
    if pd.isna(value):
        return ""
    value_str = str(value).strip()
    if not value_str:
        return ""
    try:
        numeric: float = float(value_str)
    except ValueError:
        return value_str
    if pd.isna(numeric) or not math.isfinite(numeric):
        return ""
    numeric_int = math.trunc(numeric)
    if numeric == numeric_int:
        return str(numeric_int)
    return format(numeric, "g")



def join_activity_with_molecule(
    activity_ids: Sequence[str] | pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """
    Связать activity с compound_record и molecule данными.

    Возвращает DataFrame с колонками:
      - activity_id
      - molecule_key (molecule_chembl_id)
      - molecule_name (pref_name -> первый синоним -> molecule_key)
      - compound_key (из compound_record)
      - compound_name (из compound_record)
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_molecule_join")

    # 1) Получаем/проверяем входной фрейм активностей
    if isinstance(activity_ids, pd.DataFrame):
        df_act = activity_ids.copy()
        if df_act.empty:
            log.debug("join_skipped_empty_dataframe")
            return _create_empty_result()
        required_cols = ["activity_id", "record_id", "molecule_chembl_id"]
        missing_cols = [c for c in required_cols if c not in df_act.columns]
        if missing_cols:
            log.warning("join_skipped_missing_columns", missing_columns=missing_cols)
            return _create_empty_result()
    else:
        # fallback-загрузка активностей тонким запросом (with only=...) и корректной пагинацией
        df_act = _fetch_activity_by_ids(activity_ids, client, cfg, log)
        if df_act.empty:
            log.debug("join_skipped_no_activity_data")
            return _create_empty_result()

    # 2) Cобираем уникальные ключи с нормализацией
    record_ids: set[str] = set()
    molecule_ids: set[str] = set()

    for _, row in df_act.iterrows():
        rec = row.get("record_id")
        rec_s = _canonical_record_id(rec)
        if rec_s:
            record_ids.add(rec_s)

        mol = _normalize_chembl_id(row.get("molecule_chembl_id"))
        if mol:
            molecule_ids.add(mol)

    # 3) Ранние возвраты/пустые кейсы
    if not record_ids and not molecule_ids:
        # Нормализуем минимальные выходные колонки под ожидаемую схему
        out = df_act[["activity_id"]].copy()
        out["molecule_key"] = pd.NA
        out["molecule_name"] = pd.NA
        out["compound_key"] = pd.NA
        out["compound_name"] = pd.NA
        return out[["activity_id", "molecule_key", "molecule_name", "compound_key", "compound_name"]]

    # 4) compound_record по record_id (обязательное only= и корректный items_key "compound_records")
    compound_records_dict = _fetch_compound_records_by_ids(
        list(record_ids), client, cfg, log
    )

    # 5) molecule по molecule_chembl_id (обязательное only= на
    #    molecule_chembl_id, pref_name, molecule_synonyms — это ровно те поля,
    #    которые нам нужны для имени; пагинация через page_meta)
    molecules_dict = _fetch_molecules_for_join(
        list(molecule_ids), client, cfg, log
    )

    # 6) Джоины
    df_result = _perform_joins(df_act, compound_records_dict, molecules_dict, log)

    # 7) Детерминизм: стабильно сортируем
    if "activity_id" in df_result.columns:
        df_result = df_result.sort_values("activity_id").reset_index(drop=True)

    log.info(
        "join_completed",
        rows=len(df_result),
        compound_records_matched=len(compound_records_dict),
        molecules_matched=len(molecules_dict),
    )
    return df_result


def _fetch_activity_by_ids(
    activity_ids: Sequence[str],
    client: ChemblClient,
    cfg: Mapping[str, Any],
    log: Any,
) -> pd.DataFrame:
    """Загрузить activity через API по списку activity_id."""
    fields = ["activity_id", "record_id", "molecule_chembl_id"]
    batch_size = cfg.get("batch_size", 25)  # ChEMBL API limit

    # Собрать уникальные ID
    unique_ids: list[str] = []
    for act_id in activity_ids:
        if act_id and not (isinstance(act_id, float) and pd.isna(act_id)):
            unique_ids.append(str(act_id).strip())

    if not unique_ids:
        log.debug("activity.no_ids")
        return pd.DataFrame(columns=fields)

    # Используем специализированный клиент для activity
    activity_client = ChemblActivityClient(client, batch_size=batch_size)
    all_records: list[dict[str, Any]] = []

    try:
        for record in activity_client.iterate_by_ids(unique_ids, select_fields=fields):
            all_records.append(dict(record))
    except Exception as exc:
        log.warning(
            "activity.fetch_error",
            ids_count=len(unique_ids),
            error=str(exc),
            exc_info=True,
        )

    if not all_records:
        log.debug("activity.no_records_fetched")
        return pd.DataFrame(columns=fields)

    df = pd.DataFrame.from_records(all_records)  # pyright: ignore[reportUnknownMemberType]
    return df


def _fetch_compound_records_by_ids(
    record_ids: list[str],
    client: ChemblClient,
    cfg: Mapping[str, Any],
    log: Any,
) -> dict[str, dict[str, Any]]:
    """Получить compound_record по списку record_id (ChEMBL v2, only=, order_by, детерминизм)."""
    if not record_ids:
        return {}

    # Поля-источники истины для key/name приходят из /compound_record
    fields = ["record_id", "compound_key", "compound_name"]

    # Безопасные дефолты, ChEMBL допускает limit до 1000 на страницу
    # (смотри доку по пагинации и метаданным страницы page_meta).
    page_limit_cfg = cfg.get("page_limit", 1000)
    page_limit = max(1, min(int(page_limit_cfg), 1000))
    batch_size = int(cfg.get("batch_size", 100)) or 100

    # Собираем уникальные ID (строки без пустых/NaN)
    unique_ids: list[str] = []
    seen: set[str] = set()
    for rid in record_ids:
        rid_str = _canonical_record_id(rid)
        if not rid_str:
            continue
        if rid_str not in seen:
            seen.add(rid_str)
            unique_ids.append(rid_str)

    if not unique_ids:
        log.debug("compound_record.no_ids_after_cleanup")
        return {}

    all_records: list[dict[str, Any]] = []
    ids_list = unique_ids

    # Для контроля полноты: первый ответ сохранит total_count
    expected_total: int | None = None
    collected_from_api = 0

    # Обработка батчами по фильтру record_id__in
    for i in range(0, len(ids_list), batch_size):
        chunk = ids_list[i : i + batch_size]
        params: dict[str, Any] = {
            "record_id__in": ",".join(chunk),
            "limit": page_limit,
            "only": ",".join(fields),
            # Важно: стабильный порядок во избежание дрожания страниц.
            "order_by": "record_id",
        }

        try:
            first_page_seen = False
            for record in client.paginate(
                "/compound_record.json",
                params=params,
                page_size=page_limit,
                items_key="compound_records",
            ):
                # Для получения page_meta нужно сделать отдельный запрос к первой странице
                # или использовать другой подход. Сейчас просто собираем записи.
                if not first_page_seen:
                    # Попытка получить total_count из первого запроса
                    # (требует модификации paginate для возврата метаданных)
                    first_page_seen = True

                all_records.append(dict(record))
                collected_from_api += 1

        except Exception as exc:
            log.warning(
                "compound_record.fetch_error",
                chunk_size=len(chunk),
                error=str(exc),
                exc_info=True,
            )

    # Построить словарь по record_id (берём первую запись при дубликатах)
    result: dict[str, dict[str, Any]] = {}
    for record in all_records:
        rid_raw = record.get("record_id")
        rid_str = _canonical_record_id(rid_raw)
        if rid_str and rid_str not in result:
            # оставляем только требуемые поля (жёсткий only по выходу)
            result[rid_str] = {
                "record_id": rid_str,
                "compound_key": record.get("compound_key"),
                "compound_name": record.get("compound_name"),
            }

    # Логируем метрики и возможную неполноту (не валим пайплайн)
    log.info(
        "compound_record.fetch_complete",
        ids_requested=len(unique_ids),
        records_fetched=len(all_records),
        records_deduped=len(result),
        page_limit=page_limit,
        order_by="record_id",
        has_total=(expected_total is not None),
        total_count=expected_total,
        collected=collected_from_api,
    )
    if expected_total is not None and collected_from_api < expected_total:
        log.warning(
            "compound_record.incomplete_pagination",
            collected=collected_from_api,
            total_count=expected_total,
            hint="Проверьте paginate() и items_key='compound_records', а также limit<=1000.",
        )

    return result


def _fetch_molecules_for_join(
    molecule_ids: list[str],
    client: ChemblClient,
    cfg: Mapping[str, Any],
    log: Any,
) -> dict[str, dict[str, Any]]:
    """Получить molecule по списку molecule_chembl_id с полями pref_name и molecule_synonyms."""
    if not molecule_ids:
        return {}

    fields = ["molecule_chembl_id", "pref_name", "molecule_synonyms"]
    page_limit = cfg.get("page_limit", 1000)

    return client.fetch_molecules_by_ids(
        ids=molecule_ids,
        fields=list(fields),
        page_limit=page_limit,
    )


def _perform_joins(
    df_act: pd.DataFrame,
    compound_records_dict: dict[str, dict[str, Any]],
    molecules_dict: dict[str, dict[str, Any]],
    log: Any,
) -> pd.DataFrame:
    """Выполнить два left-join и сформировать выходные поля."""
    # Создать DataFrame для compound_record
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

    # Создать DataFrame для molecule с вычислением molecule_name
    molecule_data: list[dict[str, Any]] = []
    for mol_id, record in molecules_dict.items():
        molecule_name = _extract_molecule_name(record, mol_id)
        molecule_data.append({
            "molecule_chembl_id": mol_id,
            "molecule_key": mol_id,
            "molecule_name": molecule_name,
        })

    df_molecule = pd.DataFrame(molecule_data) if molecule_data else pd.DataFrame(
        columns=["molecule_chembl_id", "molecule_key", "molecule_name"]
    )

    # Сохранить исходный порядок
    original_index = df_act.index.copy()

    # Нормализовать типы данных перед merge
    df_act_normalized = df_act.copy()

    # Нормализовать record_id: преобразовать в строку для совместимости с df_compound
    if "record_id" in df_act_normalized.columns:
        df_act_normalized["record_id"] = df_act_normalized["record_id"].map(_canonical_record_id)
        df_act_normalized.loc[df_act_normalized["record_id"] == "", "record_id"] = pd.NA
        if "record_id" in df_compound.columns and not df_compound.empty:
            df_compound["record_id"] = df_compound["record_id"].map(_canonical_record_id)
            df_compound.loc[df_compound["record_id"] == "", "record_id"] = pd.NA

    # Нормализовать molecule_chembl_id: преобразовать в строку для совместимости с df_molecule
    if "molecule_chembl_id" in df_act_normalized.columns:
        # Сохранить NaN значения, преобразовать остальные в строку
        mask_na = df_act_normalized["molecule_chembl_id"].isna()
        # Преобразовать в строку, но заменить "nan" на pd.NA
        df_act_normalized["molecule_chembl_id"] = df_act_normalized["molecule_chembl_id"].astype(str)
        df_act_normalized.loc[df_act_normalized["molecule_chembl_id"] == "nan", "molecule_chembl_id"] = pd.NA
        df_act_normalized.loc[mask_na, "molecule_chembl_id"] = pd.NA
        # Убедиться, что df_molecule.molecule_chembl_id тоже строка
        if "molecule_chembl_id" in df_molecule.columns and not df_molecule.empty:
            df_molecule["molecule_chembl_id"] = df_molecule["molecule_chembl_id"].astype(str)
            # Заменить "nan" на pd.NA
            df_molecule.loc[df_molecule["molecule_chembl_id"] == "nan", "molecule_chembl_id"] = pd.NA

    # Первый join: activity.record_id → compound_record.record_id
    df_result = df_act_normalized.merge(
        df_compound,
        on=["record_id"],
        how="left",
        suffixes=("", "_compound"),
    )

    # Второй join: activity.molecule_chembl_id → molecule.molecule_chembl_id
    df_result = df_result.merge(
        df_molecule,
        on=["molecule_chembl_id"],
        how="left",
        suffixes=("", "_molecule"),
    )

    # Восстановить исходный порядок
    df_result = df_result.reindex(original_index)

    # Убедиться, что все выходные колонки присутствуют
    output_columns = ["activity_id", "molecule_key", "molecule_name", "compound_key", "compound_name"]
    for col in output_columns:
        if col not in df_result.columns:
            df_result[col] = pd.NA

    # Если molecule_key отсутствует, заполнить из molecule_chembl_id
    if "molecule_key" in df_result.columns and "molecule_chembl_id" in df_result.columns:
        molecule_key_series: pd.Series[Any] = df_result["molecule_key"]  # pyright: ignore[reportUnknownMemberType]
        molecule_key_series = molecule_key_series.fillna(df_result["molecule_chembl_id"])  # pyright: ignore[reportUnknownMemberType]
        df_result["molecule_key"] = molecule_key_series

    # Если molecule_name отсутствует, заполнить из molecule_key
    if "molecule_name" in df_result.columns and "molecule_key" in df_result.columns:
        molecule_name_series: pd.Series[Any] = df_result["molecule_name"]  # pyright: ignore[reportUnknownMemberType]
        molecule_name_series = molecule_name_series.fillna(df_result["molecule_key"])  # pyright: ignore[reportUnknownMemberType]
        df_result["molecule_name"] = molecule_name_series

    # Выбрать только выходные колонки
    available_output_cols = [col for col in output_columns if col in df_result.columns]
    df_result = df_result[available_output_cols]

    # Нормализовать типы
    for col in ["molecule_key", "molecule_name", "compound_key", "compound_name"]:
        if col in df_result.columns:
            df_result[col] = df_result[col].astype("string")

    return df_result


def _extract_molecule_name(record: dict[str, Any], fallback_id: str) -> str:
    """Извлечь molecule_name из record с fallback логикой.

    molecule_name := coalesce(
        molecule.pref_name,
        first(molecule.molecule_synonyms[].molecule_synonym),
        molecule_key
    )
    """
    # Приоритет 1: pref_name
    pref_name = record.get("pref_name")
    if pref_name and not pd.isna(pref_name):
        pref_name_str = str(pref_name).strip()
        if pref_name_str:
            return pref_name_str

    # Приоритет 2: первый элемент из molecule_synonyms
    synonyms = record.get("molecule_synonyms")
    if synonyms:
        if isinstance(synonyms, list) and len(synonyms) > 0:  # type: ignore[arg-type]
            # Если список объектов с полем molecule_synonym
            first_syn: Any = synonyms[0]  # type: ignore[assignment]
            if isinstance(first_syn, dict):
                first_syn_dict: Mapping[str, Any] = cast(Mapping[str, Any], first_syn)
                synonym_value: Any = first_syn_dict.get("molecule_synonym")
                if synonym_value:
                    return str(synonym_value).strip()
            # Если список строк
            elif isinstance(first_syn, str):
                return first_syn.strip()
        # Если словарь
        elif isinstance(synonyms, dict):
            synonyms_dict: Mapping[str, Any] = cast(Mapping[str, Any], synonyms)
            synonym_value_from_dict: Any = synonyms_dict.get("molecule_synonym")
            if synonym_value_from_dict:
                if isinstance(synonym_value_from_dict, list) and len(synonym_value_from_dict) > 0:  # type: ignore[arg-type]
                    first_val: Any = synonym_value_from_dict[0]  # type: ignore[assignment]
                    if isinstance(first_val, dict):
                        first_val_dict: Mapping[str, Any] = cast(Mapping[str, Any], first_val)
                        return str(first_val_dict.get("molecule_synonym", "")).strip()
                    return str(first_val).strip()  # type: ignore[arg-type]
                return str(synonym_value_from_dict).strip()  # type: ignore[arg-type]

    # Приоритет 3: fallback на molecule_key
    return fallback_id


def _create_empty_result() -> pd.DataFrame:
    """Создать пустой DataFrame с правильными колонками."""
    return pd.DataFrame(
        columns=[
            "activity_id",
            "molecule_key",
            "molecule_name",
            "compound_key",
            "compound_name",
        ]
    )

