"""Base classes for ChEMBL entity fetching."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Protocol

import pandas as pd

from bioetl.clients.chembl_config import EntityConfig
from bioetl.core.logging import UnifiedLogger

__all__ = [
    "ChemblClientProtocol",
    "ChemblEntityClientProtocol",
    "ChemblEntityFetcherBase",
    "EntityConfig",
]


class ChemblClientProtocol(Protocol):
    """Минимальный контракт для ChEMBL-клиента, используемого entity-фетчерами.

    Протокол определяет минимальный интерфейс, который должен реализовывать
    клиент ChEMBL для работы с базовыми классами получения сущностей.
    Используется для обеспечения слабой связанности между компонентами.

    Methods
    -------
    paginate(endpoint, *, params=None, page_size=200, items_key=None)
        Итерирует по страницам ответов ChEMBL API.
    """

    def paginate(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 200,
        items_key: str | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Итерирует по страницам ответов ChEMBL API.

        Parameters
        ----------
        endpoint : str
            Относительный путь к endpoint API.
        params : Mapping[str, Any] | None, optional
            Параметры запроса. По умолчанию None.
        page_size : int, optional
            Размер страницы для пагинации. По умолчанию 200.
        items_key : str | None, optional
            Ключ в JSON-ответе, содержащий массив записей. По умолчанию None.

        Yields
        ------
        Mapping[str, Any]
            Отдельные записи из ответов API.
        """
        ...


class ChemblEntityClientProtocol(Protocol):
    """Общий протокол, реализуемый тонкими клиентами ChEMBL-сущностей.

    Протокол определяет стандартный интерфейс для всех клиентов сущностей ChEMBL.
    Обеспечивает единообразный API для получения данных по идентификаторам,
    полного получения всех записей и итерации по записям.

    Methods
    -------
    fetch_by_ids(ids, fields=None, *, page_limit=None)
        Получает записи сущности по идентификаторам.
    fetch_all(*, limit=None, fields=None, page_size=None)
        Получает все записи сущности с опциональными ограничениями.
    iterate_records(*, params=None, limit=None, fields=None, page_size=None)
        Итерирует по записям сущности.
    """

    def fetch_by_ids(
        self,
        ids: Sequence[str],
        fields: Sequence[str] | None = None,
        *,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """Получает записи сущности по идентификаторам.

        Parameters
        ----------
        ids : Sequence[str]
            Последовательность идентификаторов для запроса.
        fields : Sequence[str] | None, optional
            Список полей для включения в результат. Если None, возвращаются
            все поля или поля по умолчанию из конфигурации. По умолчанию None.
        page_limit : int | None, optional
            Максимальное количество записей на странице. По умолчанию None.

        Returns
        -------
        pd.DataFrame
            DataFrame с записями сущности. Колонки упорядочены согласно
            конфигурации и запрошенным полям.

        Raises
        ------
        TypeError
            Если ids не является последовательностью или содержит нестроковые значения.
        """
        ...

    def fetch_all(
        self,
        *,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> pd.DataFrame:
        """Получает все записи сущности с опциональными ограничениями.

        Parameters
        ----------
        limit : int | None, optional
            Максимальное количество записей для возврата. По умолчанию None.
        fields : Sequence[str] | None, optional
            Список полей для включения в результат. По умолчанию None.
        page_size : int | None, optional
            Размер страницы для пагинации. По умолчанию None.

        Returns
        -------
        pd.DataFrame
            DataFrame со всеми записями сущности (с учётом limit).
        """
        ...

    def iterate_records(
        self,
        *,
        params: Mapping[str, Any] | None = None,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Итерирует по записям сущности.

        Parameters
        ----------
        params : Mapping[str, Any] | None, optional
            Дополнительные параметры запроса. По умолчанию None.
        limit : int | None, optional
            Максимальное количество записей для возврата. По умолчанию None.
        fields : Sequence[str] | None, optional
            Список полей для включения в результат. По умолчанию None.
        page_size : int | None, optional
            Размер страницы для пагинации. По умолчанию None.

        Yields
        ------
        Mapping[str, Any]
            Отдельные записи сущности в виде словарей.
        """
        ...


class ChemblEntityFetcherBase(ChemblEntityClientProtocol):
    """Базовый класс для получения ChEMBL-сущностей, возвращающий DataFrame.

    Предоставляет общую реализацию методов протокола ChemblEntityClientProtocol:
    разбиение идентификаторов на чанки, построение параметров запросов,
    преобразование записей в DataFrame с детерминированным порядком колонок
    и сортировкой.

    Parameters
    ----------
    chembl_client : ChemblClientProtocol
        Клиент ChEMBL для выполнения HTTP-запросов.
    config : EntityConfig
        Конфигурация сущности, определяющая endpoint, параметры фильтрации
        и настройки пагинации.

    Attributes
    ----------
    _DEFAULT_PAGE_SIZE : int
        Размер страницы по умолчанию (1000).
    """

    _DEFAULT_PAGE_SIZE = 1000

    def __init__(self, chembl_client: ChemblClientProtocol, config: EntityConfig) -> None:
        """Инициализирует фетчер для ChEMBL-сущности.

        Parameters
        ----------
        chembl_client : ChemblClientProtocol
            Клиент ChEMBL для выполнения HTTP-запросов.
        config : EntityConfig
            Конфигурация сущности.
        """
        self._chembl_client: ChemblClientProtocol = chembl_client
        self._config = config
        self._log = UnifiedLogger.get(__name__).bind(
            component="chembl_entity",
            entity=config.log_prefix,
        )

    def fetch_by_ids(
        self,
        ids: Sequence[str],
        fields: Sequence[str] | None = None,
        *,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """Получает записи сущности по идентификаторам.

        Разбивает идентификаторы на чанки согласно config.chunk_size,
        выполняет запросы для каждого чанка и объединяет результаты
        в единый DataFrame с детерминированным порядком колонок.

        Parameters
        ----------
        ids : Sequence[str]
            Последовательность идентификаторов для запроса.
        fields : Sequence[str] | None, optional
            Список полей для включения в результат. По умолчанию None.
        page_limit : int | None, optional
            Максимальное количество записей на странице. По умолчанию None.

        Returns
        -------
        pd.DataFrame
            DataFrame с записями сущности. Пустой DataFrame с правильными
            колонками, если ids пуст или не содержит валидных идентификаторов.

        Raises
        ------
        TypeError
            Если ids не является последовательностью или содержит нестроковые значения.
        """
        identifiers = self._validate_identifiers(ids)
        if not identifiers:
            self._log.debug(f"{self._config.log_prefix}.fetch_by_ids.no_ids")
            return self._empty_frame(fields)

        page_size = self._resolve_page_size(page_limit, None)
        records: list[Mapping[str, Any]] = []
        for chunk in self._iter_chunks(identifiers):
            params = self._build_chunk_params(chunk, fields=fields)
            chunk_records = list(
                self.iterate_records(
                    params=params,
                    fields=fields,
                    page_size=page_size,
                )
            )
            records.extend(chunk_records)

        frame = self._records_to_frame(records, fields)
        self._log.info(
            f"{self._config.log_prefix}.fetch_by_ids.complete",
            ids_requested=len(identifiers),
            rows=len(frame),
        )
        return frame

    def fetch_all(
        self,
        *,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> pd.DataFrame:
        """Получает все записи сущности с опциональными ограничениями.

        Выполняет запрос всех записей через iterate_records и преобразует
        результаты в DataFrame с детерминированным порядком колонок.

        Parameters
        ----------
        limit : int | None, optional
            Максимальное количество записей для возврата. По умолчанию None.
        fields : Sequence[str] | None, optional
            Список полей для включения в результат. По умолчанию None.
        page_size : int | None, optional
            Размер страницы для пагинации. По умолчанию None.

        Returns
        -------
        pd.DataFrame
            DataFrame со всеми записями сущности (с учётом limit).
        """
        effective_page_size = self._resolve_page_size(page_size, limit)
        records = list(
            self.iterate_records(
                limit=limit,
                fields=fields,
                page_size=effective_page_size,
            )
        )
        frame = self._records_to_frame(records, fields)
        self._log.info(
            f"{self._config.log_prefix}.fetch_all.complete",
            rows=len(frame),
            limit=limit,
        )
        return frame

    def iterate_records(
        self,
        *,
        params: Mapping[str, Any] | None = None,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Итерирует по записям сущности.

        Использует chembl_client.paginate для получения записей с пагинацией.
        Применяет фильтры по умолчанию из config, добавляет параметры fields
        и ограничивает количество записей согласно limit.

        Parameters
        ----------
        params : Mapping[str, Any] | None, optional
            Дополнительные параметры запроса, объединяемые с фильтрами
            по умолчанию из config. По умолчанию None.
        limit : int | None, optional
            Максимальное количество записей для возврата. По умолчанию None.
        fields : Sequence[str] | None, optional
            Список полей для включения в результат (параметр "only").
            По умолчанию None.
        page_size : int | None, optional
            Размер страницы для пагинации. По умолчанию None.

        Yields
        ------
        Mapping[str, Any]
            Отдельные записи сущности в виде словарей.
        """
        resolved_page_size = self._resolve_page_size(page_size, limit)
        request_params = self._compose_params(
            params=params,
            fields=fields,
            page_size=resolved_page_size,
        )
        yielded = 0
        for record in self._chembl_client.paginate(
            self._config.endpoint,
            params=request_params,
            page_size=resolved_page_size,
            items_key=self._config.items_key,
        ):
            yield dict(record)
            yielded += 1
            if limit is not None and yielded >= limit:
                break

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    def _validate_identifiers(self, ids: Sequence[str]) -> list[str]:
        if not isinstance(ids, Sequence):
            msg = "ids must be a sequence of strings"
            raise TypeError(msg)
        identifiers: list[str] = []
        for idx, identifier in enumerate(ids):
            if not isinstance(identifier, str):
                msg = f"identifier at position {idx} must be str, got {type(identifier)!r}"
                raise TypeError(msg)
            if identifier:
                identifiers.append(identifier)
        return identifiers

    def _iter_chunks(self, identifiers: Sequence[str]) -> Iterator[Sequence[str]]:
        chunk_size = self._config.chunk_size
        for offset in range(0, len(identifiers), chunk_size):
            yield identifiers[offset : offset + chunk_size]

    def _compose_params(
        self,
        *,
        params: Mapping[str, Any] | None,
        fields: Sequence[str] | None,
        page_size: int,
    ) -> dict[str, Any]:
        request_params: dict[str, Any] = dict(self._config.filters)
        if params:
            request_params.update(params)
        if fields:
            request_params["only"] = ",".join(fields)
        request_params.setdefault("limit", page_size)
        return request_params

    def _resolve_page_size(
        self,
        requested: int | None,
        limit: int | None,
    ) -> int:
        candidate = requested or self._config.max_page_size or self._DEFAULT_PAGE_SIZE
        if candidate <= 0:
            candidate = self._DEFAULT_PAGE_SIZE
        if limit is not None:
            candidate = min(candidate, max(limit, 1))
        max_page_size = self._config.max_page_size
        if max_page_size is not None:
            candidate = min(candidate, max_page_size)
        return candidate

    def _records_to_frame(
        self,
        records: Sequence[Mapping[str, Any]],
        fields: Sequence[str] | None,
    ) -> pd.DataFrame:
        if not records:
            return self._empty_frame(fields)

        frame = pd.DataFrame.from_records(records)
        ordered_columns = self._resolve_column_order(frame.columns.tolist(), fields)
        frame = frame.reindex(columns=ordered_columns)
        if self._config.ordering:
            sort_columns = [
                column for column in self._config.ordering if column in frame.columns
            ]
            if sort_columns:
                frame = frame.sort_values(by=sort_columns, kind="mergesort")
        return frame.reset_index(drop=True)

    def _resolve_column_order(
        self,
        columns: Sequence[str],
        fields: Sequence[str] | None,
    ) -> list[str]:
        ordered: list[str] = []

        def _extend(values: Sequence[str]) -> None:
            for value in values:
                if value not in ordered:
                    ordered.append(value)

        if fields:
            _extend(fields)
        elif self._config.default_fields:
            _extend(self._config.default_fields)

        if self._config.ordering:
            _extend(self._config.ordering)

        _extend(columns)
        return ordered

    def _empty_frame(self, fields: Sequence[str] | None) -> pd.DataFrame:
        column_order = list(fields) if fields else list(self._config.default_fields)
        if not column_order:
            column_order = list(self._config.ordering)
        return pd.DataFrame(columns=column_order)

    def _build_chunk_params(
        self,
        chunk: Sequence[str],
        *,
        fields: Sequence[str] | None,
    ) -> dict[str, Any]:
        return {self._config.filter_param: ",".join(chunk)}

