"""Chembl compound record entity client."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from bioetl.clients.client_chembl_base import ChemblClientProtocol
from bioetl.core.log_events import LogEvents
from bioetl.core.logger import UnifiedLogger

__all__ = ["ChemblCompoundRecordEntityClient"]


class ChemblCompoundRecordEntityClient:
    """Клиент для получения compound_record записей из ChEMBL API.

    Особенность: использует пары (molecule_chembl_id, document_chembl_id)
    вместо простых ID, поэтому не наследуется от ChemblEntityFetcher.
    """

    def __init__(self, chembl_client: ChemblClientProtocol) -> None:
        """Инициализировать клиент для compound_record.

        Parameters
        ----------
        chembl_client:
            Экземпляр ChemblClient для выполнения запросов.
        """
        self._chembl_client = chembl_client
        self._log = UnifiedLogger.get(__name__).bind(component="compound_record")

    def fetch_by_pairs(
        self,
        pairs: Iterable[tuple[str, str]],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        """Получить compound_record записи по парам (molecule_chembl_id, document_chembl_id).

        Parameters
        ----------
        pairs:
            Итерируемый объект с парами (molecule_chembl_id, document_chembl_id).
        fields:
            Список полей для получения из API.
        page_limit:
            Размер страницы для пагинации.

        Returns
        -------
        dict[tuple[str, str], dict[str, Any]]:
            Словарь (molecule_chembl_id, document_chembl_id) -> запись.
        """
        import pandas as pd

        # Сбор уникальных пар, фильтрация None/NA значений
        unique_pairs: set[tuple[str, str]] = set()
        for mol_id, doc_id in pairs:
            if (
                mol_id
                and doc_id
                and not (isinstance(mol_id, float) and pd.isna(mol_id))
                and not (isinstance(doc_id, float) and pd.isna(doc_id))
            ):
                unique_pairs.add((str(mol_id).strip(), str(doc_id).strip()))

        if not unique_pairs:
            self._log.debug(LogEvents.COMPOUND_RECORD_NO_PAIRS, message="No valid pairs to fetch")
            return {}

        # Группировка пар по document_chembl_id
        doc_to_molecules: dict[str, list[str]] = {}
        for mol_id, doc_id in unique_pairs:
            doc_to_molecules.setdefault(doc_id, []).append(mol_id)

        # Получение записей, сгруппированных по документу
        all_records: list[dict[str, Any]] = []
        for doc_id, mol_ids in doc_to_molecules.items():
            # ChEMBL API поддерживает molecule_chembl_id__in фильтр
            # Обработка чанками для избежания ограничений длины URL
            chunk_size = 100  # Консервативный лимит для molecule_chembl_id__in
            for i in range(0, len(mol_ids), chunk_size):
                chunk = mol_ids[i : i + chunk_size]
                params: dict[str, Any] = {
                    "document_chembl_id": doc_id,
                    "molecule_chembl_id__in": ",".join(chunk),
                    "limit": page_limit,
                }
                # Параметр only для выбора полей
                if fields:
                    params["only"] = ",".join(fields)

                try:
                    for record in self._chembl_client.paginate(
                        "/compound_record.json",
                        params=params,
                        page_size=page_limit,
                        items_key="compound_records",
                    ):
                        all_records.append(dict(record))
                except Exception as exc:
                    self._log.warning(
                        LogEvents.COMPOUND_RECORD_FETCH_ERROR,
                        document_chembl_id=doc_id,
                        molecule_count=len(chunk),
                        error=str(exc),
                        exc_info=True,
                    )

        # Дедупликация записей по (molecule_chembl_id, document_chembl_id)
        # Приоритет: curated=True > False; removed=False > True; min record_id
        result: dict[tuple[str, str], dict[str, Any]] = {}
        for record in all_records:
            mol_id_raw = record.get("molecule_chembl_id")
            doc_id_raw = record.get("document_chembl_id")
            if not mol_id_raw or not doc_id_raw:
                continue
            if not isinstance(mol_id_raw, str) or not isinstance(doc_id_raw, str):
                continue
            mol_id = mol_id_raw
            doc_id = doc_id_raw

            key = (mol_id, doc_id)
            existing = result.get(key)

            if existing is None:
                result[key] = record
            else:
                result[key] = _compound_record_dedup_priority(existing, record)

        self._log.info(
            LogEvents.COMPOUND_RECORD_FETCH_COMPLETE,
            pairs_requested=len(unique_pairs),
            records_fetched=len(all_records),
            records_deduped=len(result),
        )
        return result


def _compound_record_dedup_priority(
    existing: dict[str, Any],
    new: dict[str, Any],
) -> dict[str, Any]:
    """Функция приоритета для дедупликации compound_record.

    Приоритет:
    1. curated=True > False
    2. removed=False > True
    3. min record_id

    Parameters
    ----------
    existing:
        Существующая запись.
    new:
        Новая запись.

    Returns
    -------
    dict[str, Any]:
        Выбранная запись.
    """
    existing_curated = _safe_bool(existing.get("curated"))
    new_curated = _safe_bool(new.get("curated"))
    existing_removed = _safe_bool(existing.get("removed"))
    new_removed = _safe_bool(new.get("removed"))

    # Priority 1: curated=True > False
    if new_curated and not existing_curated:
        return new
    if existing_curated and not new_curated:
        return existing

    # Priority 2: removed=False > True
    if not new_removed and existing_removed:
        return new
    if not existing_removed and new_removed:
        return existing

    # Priority 3: min record_id
    existing_id = existing.get("record_id")
    new_id = new.get("record_id")
    if existing_id is not None and new_id is not None:
        try:
            if int(new_id) < int(existing_id):
                return new
        except (ValueError, TypeError):
            pass

    return existing


def _safe_bool(value: Any) -> bool:
    """Преобразовать значение в bool безопасно.

    Обрабатывает 0/1, None и boolean значения.

    Parameters
    ----------
    value:
        Значение для преобразования.

    Returns
    -------
    bool:
        Преобразованное значение.
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value) and value != 0
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)

