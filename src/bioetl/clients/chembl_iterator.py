"""Base class for ChEMBL entity iterators."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, cast
from urllib.parse import urlencode

from bioetl.clients.chembl_base import EntityConfig
from bioetl.core.logger import UnifiedLogger

# ChemblClient is dynamically loaded in __init__.py, so we use Any for type checking
# Import is done at runtime to avoid circular dependencies

__all__ = ["ChemblEntityIterator"]


class ChemblEntityIterator:
    """Базовый класс для итерации по сущностям ChEMBL.

    Предоставляет унифицированный интерфейс для итерации по записям
    различных типов сущностей ChEMBL API с поддержкой пагинации,
    чанкинга и проверки длины URL.
    """

    def __init__(
        self,
        chembl_client: Any,
        config: EntityConfig,
        *,
        batch_size: int,
        max_url_length: int | None = None,
    ) -> None:
        """Инициализировать итератор для сущности.

        Parameters
        ----------
        chembl_client:
            Экземпляр ChemblClient для выполнения запросов.
        config:
            Конфигурация сущности.
        batch_size:
            Размер батча для пагинации (максимум 25 для ChEMBL API).
        max_url_length:
            Максимальная длина URL для проверки. Если None, проверка отключена.
        """
        if batch_size <= 0:
            msg = "batch_size must be a positive integer"
            raise ValueError(msg)
        if max_url_length is not None and max_url_length <= 0:
            msg = "max_url_length must be a positive integer if provided"
            raise ValueError(msg)

        self._chembl_client = chembl_client
        # Backwards compatibility for client attribute expected by legacy code/tests
        self._client = chembl_client
        self._config = config
        self._batch_size = min(batch_size, 25)
        self._max_url_length = max_url_length
        self._chembl_release: str | None = None
        self._log = UnifiedLogger.get(__name__).bind(
            component="chembl_iterator",
            entity=config.log_prefix,
        )

    @property
    def chembl_release(self) -> str | None:
        """Вернуть ChEMBL release, полученный во время handshake.

        Returns
        -------
        str | None:
            Версия ChEMBL release или None, если handshake не выполнялся.
        """
        return self._chembl_release

    @property
    def chembl_client(self) -> Any:
        """Вернуть обёрнутый ChemblClient."""

        return self._chembl_client

    @property
    def batch_size(self) -> int:
        """Вернуть текущий размер батча для пагинации."""

        return self._batch_size

    @property
    def max_url_length(self) -> int | None:
        """Вернуть ограничение длины URL (если задано)."""

        return self._max_url_length

    def handshake(
        self,
        *,
        endpoint: str = "/status",
        enabled: bool = True,
    ) -> Mapping[str, object]:
        """Выполнить handshake и кэшировать идентификатор release.

        Parameters
        ----------
        endpoint:
            Endpoint для handshake. По умолчанию "/status".
        enabled:
            Если False, handshake не выполняется. По умолчанию True.

        Returns
        -------
        Mapping[str, object]:
            Payload ответа от handshake или пустой словарь, если disabled.
        """
        if not enabled:
            return {}

        payload = self._chembl_client.handshake(endpoint)
        release = payload.get("chembl_db_version")
        if isinstance(release, str):
            self._chembl_release = release

        return cast(Mapping[str, object], payload)

    def iterate_all(
        self,
        *,
        limit: int | None = None,
        page_size: int | None = None,
        select_fields: Sequence[str] | None = None,
    ) -> Iterator[Mapping[str, object]]:
        """Итерировать по всем записям сущности с поддержкой лимитов.

        Parameters
        ----------
        limit:
            Максимальное количество записей для возврата.
        page_size:
            Размер страницы для пагинации. Если None, используется batch_size.
        select_fields:
            Опциональный список полей для получения через параметр `only`.

        Yields
        ------
        Mapping[str, object]:
            Записи сущности.
        """
        effective_page_size = self._coerce_page_size(page_size)
        yielded = 0

        params: dict[str, object] = {}
        if limit is not None and limit > 0:
            params["limit"] = min(effective_page_size, limit)
        else:
            params["limit"] = effective_page_size

        if select_fields:
            params["only"] = ",".join(select_fields)

        for item in self._chembl_client.paginate(
            self._config.endpoint,
            params=params,
            page_size=effective_page_size,
            items_key=self._config.items_key,
        ):
            yield item
            yielded += 1
            if limit is not None and yielded >= limit:
                break

    def iterate_by_ids(
        self,
        ids: Sequence[str],
        *,
        select_fields: Sequence[str] | None = None,
    ) -> Iterator[Mapping[str, object]]:
        """Итерировать по записям сущности по конкретным ID с умным чанкингом.

        Parameters
        ----------
        ids:
            Последовательность ID для получения.
        select_fields:
            Опциональный список полей для получения через параметр `only`.

        Yields
        ------
        Mapping[str, object]:
            Записи сущности.
        """
        for chunk in self._chunk_identifiers(ids, select_fields=select_fields):
            params: dict[str, object] = {self._config.filter_param: ",".join(chunk)}
            if select_fields:
                params["only"] = ",".join(select_fields)

            yield from self._chembl_client.paginate(
                self._config.endpoint,
                params=params,
                page_size=len(chunk),
                items_key=self._config.items_key,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _coerce_page_size(self, requested: int | None) -> int:
        """Привести размер страницы к допустимому значению.

        Parameters
        ----------
        requested:
            Запрошенный размер страницы. Если None, используется batch_size.

        Returns
        -------
        int:
            Эффективный размер страницы.
        """
        if requested is None:
            return self._batch_size
        if requested <= 0:
            return self._batch_size
        return min(requested, self._batch_size)

    def _chunk_identifiers(
        self,
        ids: Sequence[object],
        *,
        select_fields: Sequence[str] | None = None,
    ) -> Iterable[Sequence[str]]:
        """Разбить идентификаторы на чанки с учетом длины URL.

        Parameters
        ----------
        ids:
            Последовательность идентификаторов (любые объекты) для разбиения.
        select_fields:
            Опциональный список полей для расчета длины URL.

        Yields
        ------
        Sequence[str]:
            Чанки идентификаторов.
        """
        chunk: deque[str] = deque()

        for identifier in ids:
            if identifier is None:
                continue
            if isinstance(identifier, str):
                candidate_identifier = identifier.strip()
            else:
                candidate_identifier = str(identifier).strip()
            if not candidate_identifier:
                continue

            candidate_size = len(chunk) + 1

            # Проверка длины URL, если включена
            if self._config.enable_url_length_check and self._max_url_length is not None:
                candidate_param_length = self._encode_in_query(
                    tuple(list(chunk) + [candidate_identifier]),
                    select_fields=select_fields,
                )
                if (
                    candidate_size > self._batch_size
                    or candidate_param_length > self._max_url_length
                ):
                    if chunk:
                        yield tuple(chunk)
                        chunk.clear()
                    chunk.append(candidate_identifier)
                    continue
            elif candidate_size > self._batch_size:
                # Простая проверка размера чанка без проверки длины URL
                if chunk:
                    yield tuple(chunk)
                    chunk.clear()
                chunk.append(candidate_identifier)
                continue

            chunk.append(candidate_identifier)

        if chunk:
            yield tuple(chunk)

    def _encode_in_query(
        self,
        identifiers: Sequence[str],
        *,
        select_fields: Sequence[str] | None = None,
    ) -> int:
        """Вычислить длину параметров запроса в URL.

        Parameters
        ----------
        identifiers:
            Последовательность идентификаторов.
        select_fields:
            Опциональный список полей.

        Returns
        -------
        int:
            Приблизительная длина параметров запроса в URL.
        """
        params_dict: dict[str, str] = {self._config.filter_param: ",".join(identifiers)}
        if select_fields:
            params_dict["only"] = ",".join(select_fields)

        params = urlencode(params_dict)
        # Учитываем базовую длину endpoint для приблизительной оценки финальной длины URL
        base_length = self._config.base_endpoint_length or len(self._config.endpoint)
        return base_length + len("?") + len(params)
