"""Base classes for ChEMBL entity fetching."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pandas as pd

from bioetl.core.logger import UnifiedLogger

if TYPE_CHECKING:
    from bioetl.clients import ChemblClient

__all__ = ["EntityConfig", "ChemblEntityFetcher"]


@dataclass(frozen=True, kw_only=True)
class EntityConfig:
    """Конфигурация для работы с сущностью ChEMBL.

    Attributes
    ----------
    endpoint:
        API endpoint для сущности, например "/assay.json".
    filter_param:
        Имя параметра фильтрации, например "assay_chembl_id__in".
    id_key:
        Ключ ID в записи, например "assay_chembl_id".
    items_key:
        Ключ массива записей в ответе API, например "assays".
    log_prefix:
        Префикс для логирования, например "assay".
    chunk_size:
        Размер чанка для батчинга запросов. По умолчанию 100.
    supports_list_result:
        Если True, результат может содержать списки записей для одного ID.
        По умолчанию False (один объект на ID).
    dedup_priority:
        Функция для дедупликации записей при конфликте.
        Принимает (existing, new) и возвращает выбранную запись.
        По умолчанию None (используется последняя запись).
    """

    endpoint: str
    filter_param: str
    id_key: str
    items_key: str
    log_prefix: str
    chunk_size: int = 100
    supports_list_result: bool = False
    dedup_priority: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None = None
    base_endpoint_length: int = 0  # Базовая длина endpoint для расчета длины URL
    enable_url_length_check: bool = False  # Включить проверку длины URL


class ChemblEntityFetcher:
    """Базовый класс для получения сущностей ChEMBL по ID.

    Предоставляет унифицированный интерфейс для работы с различными
    типами сущностей ChEMBL API.
    """

    def __init__(self, chembl_client: ChemblClient, config: EntityConfig) -> None:  # type: ignore[valid-type]
        """Инициализировать fetcher для сущности.

        Parameters
        ----------
        chembl_client:
            Экземпляр ChemblClient для выполнения запросов.
        config:
            Конфигурация сущности.
        """
        self._chembl_client = chembl_client
        self._config = config
        self._log = UnifiedLogger.get(__name__).bind(
            component="chembl_entity",
            entity=config.log_prefix,
        )

    def fetch_by_ids(
        self,
        ids: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[str, dict[str, Any]] | dict[str, list[dict[str, Any]]]:
        """Получить записи сущности по ID.

        Parameters
        ----------
        ids:
            Итерируемый объект с ID для получения.
        fields:
            Список полей для получения из API.
        page_limit:
            Размер страницы для пагинации.

        Returns
        -------
        dict[str, dict[str, Any]] | dict[str, list[dict[str, Any]]]:
            Словарь, ключ - ID, значение - запись или список записей
            (в зависимости от supports_list_result).
        """
        # Нормализация и фильтрация ID
        unique_ids: set[str] = set()
        for entity_id in ids:
            if entity_id and not (isinstance(entity_id, float) and pd.isna(entity_id)):
                unique_ids.add(str(entity_id).strip())

        if not unique_ids:
            self._log.debug(
                f"{self._config.log_prefix}.no_ids",
                message="No valid IDs to fetch",
            )
            if self._config.supports_list_result:
                return {}
            return {}

        # Обработка чанками
        all_records: list[dict[str, Any]] = []
        ids_list = list(unique_ids)

        for i in range(0, len(ids_list), self._config.chunk_size):
            chunk = ids_list[i : i + self._config.chunk_size]
            params: dict[str, Any] = {
                self._config.filter_param: ",".join(chunk),
                "limit": page_limit,
            }
            # Параметр only для выбора полей
            if fields:
                params["only"] = ",".join(fields)

            try:
                for record in self._chembl_client.paginate(  # type: ignore[attr-defined]
                    self._config.endpoint,
                    params=params,
                    page_size=page_limit,
                    items_key=self._config.items_key,
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                self._log.warning(
                    f"{self._config.log_prefix}.fetch_error",
                    entity_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        # Построение результата
        if self._config.supports_list_result:
            return self._build_list_result(all_records, unique_ids)
        return self._build_dict_result(all_records, unique_ids)

    def _build_dict_result(
        self,
        records: list[dict[str, Any]],
        unique_ids: set[str],
    ) -> dict[str, dict[str, Any]]:
        """Построить словарь результат (один объект на ID).

        Parameters
        ----------
        records:
            Список всех полученных записей.
        unique_ids:
            Множество запрошенных ID.

        Returns
        -------
        dict[str, dict[str, Any]]:
            Словарь ID -> запись.
        """
        result: dict[str, dict[str, Any]] = {}
        for record in records:
            entity_id_raw = record.get(self._config.id_key)
            if not entity_id_raw:
                continue
            if not isinstance(entity_id_raw, str):
                continue
            entity_id = entity_id_raw

            if entity_id not in result:
                result[entity_id] = record
            elif self._config.dedup_priority:
                # Использовать функцию приоритета для дедупликации
                existing = result[entity_id]
                result[entity_id] = self._config.dedup_priority(existing, record)

        self._log.info(
            f"{self._config.log_prefix}.fetch_complete",
            ids_requested=len(unique_ids),
            records_fetched=len(records),
            records_deduped=len(result),
        )
        return result

    def _build_list_result(
        self,
        records: list[dict[str, Any]],
        unique_ids: set[str],
    ) -> dict[str, list[dict[str, Any]]]:
        """Построить словарь результат (список объектов на ID).

        Parameters
        ----------
        records:
            Список всех полученных записей.
        unique_ids:
            Множество запрошенных ID.

        Returns
        -------
        dict[str, list[dict[str, Any]]]:
            Словарь ID -> список записей.
        """
        result: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            entity_id_raw = record.get(self._config.id_key)
            if not entity_id_raw:
                continue
            if not isinstance(entity_id_raw, str):
                continue
            entity_id = entity_id_raw
            if entity_id not in result:
                result[entity_id] = []
            result[entity_id].append(record)

        self._log.info(
            f"{self._config.log_prefix}.fetch_complete",
            ids_requested=len(unique_ids),
            records_fetched=len(records),
            entities_with_records=len(result),
        )
        return result
