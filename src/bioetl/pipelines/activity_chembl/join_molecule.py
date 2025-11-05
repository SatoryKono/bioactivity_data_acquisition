"""Join activity with compound_record and molecule data."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

import pandas as pd

from bioetl.clients.chembl import ChemblClient
from bioetl.core.logger import UnifiedLogger

__all__ = ["join_activity_with_molecule"]


def join_activity_with_molecule(
    activity_ids: Sequence[str] | pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Связать activity с compound_record и molecule данными.

    Загружает activity через API с полями activity_id, record_id, molecule_chembl_id,
    затем получает compound_record по record_id и molecule по molecule_chembl_id,
    выполняет два left-join и формирует выходные поля.

    Parameters
    ----------
    activity_ids:
        Последовательность activity_id для загрузки через API или DataFrame с уже
        загруженными данными activity (должен содержать activity_id, record_id, molecule_chembl_id).
    client:
        ChemblClient для запросов к ChEMBL API.
    cfg:
        Конфигурация из config.chembl.activity.join.molecule.

    Returns
    -------
    pd.DataFrame:
        DataFrame с колонками:
        - activity_id
        - molecule_key (molecule_chembl_id)
        - molecule_name (pref_name с fallback на первый синоним, затем molecule_key)
        - compound_key (из compound_record)
        - compound_name (из compound_record)
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_molecule_join")

    # Загрузить activity через API
    if isinstance(activity_ids, pd.DataFrame):
        df_act = activity_ids.copy()
        if df_act.empty:
            log.debug("join_skipped_empty_dataframe")
            return _create_empty_result()
        # Проверка наличия необходимых колонок
        required_cols = ["activity_id", "record_id", "molecule_chembl_id"]
        missing_cols = [col for col in required_cols if col not in df_act.columns]
        if missing_cols:
            log.warning("join_skipped_missing_columns", missing_columns=missing_cols)
            return _create_empty_result()
    else:
        # Загрузить activity через API
        df_act = _fetch_activity_by_ids(activity_ids, client, cfg, log)
        if df_act.empty:
            log.debug("join_skipped_no_activity_data")
            return _create_empty_result()

    # Собрать уникальные record_id и molecule_chembl_id
    record_ids: list[str] = []
    molecule_ids: list[str] = []

    for _, row in df_act.iterrows():
        record_id = row.get("record_id")
        mol_id = row.get("molecule_chembl_id")

        if record_id is not None and not pd.isna(record_id):
            record_id_str = str(record_id).strip()
            if record_id_str:
                record_ids.append(record_id_str)

        if mol_id is not None and not pd.isna(mol_id):
            mol_id_str = str(mol_id).strip()
            if mol_id_str:
                molecule_ids.append(mol_id_str)

    # Получить compound_record по record_id
    compound_records_dict = _fetch_compound_records_by_ids(
        record_ids, client, cfg, log
    )

    # Получить molecule по molecule_chembl_id
    molecules_dict = _fetch_molecules_for_join(molecule_ids, client, cfg, log)

    # Выполнить два left-join
    df_result = _perform_joins(df_act, compound_records_dict, molecules_dict, log)

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
    page_limit = cfg.get("page_limit", 1000)
    batch_size = cfg.get("batch_size", 25)  # ChEMBL API limit

    # Собрать уникальные ID
    unique_ids: set[str] = set()
    for act_id in activity_ids:
        if act_id and not (isinstance(act_id, float) and pd.isna(act_id)):
            unique_ids.add(str(act_id).strip())

    if not unique_ids:
        log.debug("activity.no_ids")
        return pd.DataFrame(columns=fields)

    all_records: list[dict[str, Any]] = []
    ids_list = list(unique_ids)

    # Обработка батчами
    for i in range(0, len(ids_list), batch_size):
        chunk = ids_list[i : i + batch_size]
        params: dict[str, Any] = {
            "activity_id__in": ",".join(chunk),
            "limit": page_limit,
            "only": ",".join(fields),
        }

        try:
            for record in client.paginate(
                "/activity.json",
                params=params,
                page_size=page_limit,
                items_key="activities",
            ):
                all_records.append(dict(record))
        except Exception as exc:
            log.warning(
                "activity.fetch_error",
                chunk_size=len(chunk),
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
    """Получить compound_record по списку record_id."""
    if not record_ids:
        return {}

    fields = ["record_id", "compound_key", "compound_name"]
    page_limit = cfg.get("page_limit", 1000)
    batch_size = cfg.get("batch_size", 100)  # Conservative limit

    unique_ids: set[str] = set(record_ids)
    all_records: list[dict[str, Any]] = []
    ids_list = list(unique_ids)

    # Обработка батчами
    for i in range(0, len(ids_list), batch_size):
        chunk = ids_list[i : i + batch_size]
        params: dict[str, Any] = {
            "record_id__in": ",".join(chunk),
            "limit": page_limit,
            "only": ",".join(fields),
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
                "compound_record.fetch_error",
                chunk_size=len(chunk),
                error=str(exc),
                exc_info=True,
            )

    # Построить словарь по record_id
    result: dict[str, dict[str, Any]] = {}
    for record in all_records:
        record_id_raw = record.get("record_id")
        if record_id_raw is None:
            continue
        record_id_str = str(record_id_raw).strip()
        if record_id_str:
            # Если есть несколько записей с одним record_id, берем первую
            if record_id_str not in result:
                result[record_id_str] = record

    log.info(
        "compound_record.fetch_complete",
        ids_requested=len(unique_ids),
        records_fetched=len(all_records),
        records_deduped=len(result),
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

    # Первый join: activity.record_id → compound_record.record_id
    df_result = df_act.merge(
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

