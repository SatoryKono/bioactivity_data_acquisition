from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    Hashable,
    Iterable,
    Mapping,
    MutableMapping,
    Protocol,
    Sequence,
    runtime_checkable,
)

Record = MutableMapping[str, Any]


@dataclass(frozen=True)
class RequestContext:
    """
    Попутный контекст, не влияющий на бизнес-семантику запроса.
    Всё, что относится к трассировке, таймаутам, ретраям и т.п.
    """

    trace_id: str | None = None
    timeout_s: float | None = None
    max_retries: int | None = None
    # Для всего, что не влезло в поля выше: заголовки, tenant-id и т.п.
    extra: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class PaginationParams:
    """
    Унифицированные параметры пагинации для всех источников.
    """

    # Глобальный лимит на количество записей, которое клиент отдаст наружу
    limit: int | None = None
    # Начальный offset / номер страницы (зависит от транспорта)
    offset: int | None = None
    # Размер "сырой" страницы при запросе к источнику
    page_size: int | None = None
    # Защита от бесконечных API: максимум страниц, которые мы готовы обойти
    max_pages: int | None = None


@dataclass(frozen=True)
class ClientRequest:
    """
    Унифицированное описание того, чего мы хотим от клиента.

    ВАЖНО: клиент *ресурсно-специфичен* (target, activity, article и т.п.),
    поэтому здесь нет имени ресурса – оно зашито в самом клиенте.
    """

    # Список идентификаторов (например, chembl_id / doi / uniprot_id)
    ids: Sequence[Hashable] | None = None

    # Абстрактные фильтры (ключи договорные, не "сырые" query params)
    filters: Mapping[str, Any] | None = None

    # Параметры пагинации
    pagination: PaginationParams | None = None

    # Свободное поле для транспорта (например, сырой запрос в OpenAlex или SQL)
    raw: Any | None = None


@dataclass(frozen=True)
class Page:
    items: Sequence[Record]
    # Следующий offset/ключ курсора, если известен
    next_offset: int | str | None = None
    has_next: bool = False


RecordStream = Iterable[Record]
PageStream = Iterable[Page]


class ClientError(Exception):
    """Базовая ошибка клиентского слоя."""


@runtime_checkable
class DataClient(Protocol):
    """
    Единый контракт для всех клиентских реализаций.

    Конкретный клиент = один логический ресурс одного источника.
    Примеры:
      - ChEMBL: target, activity, molecule
      - PubChem: substance, compound
      - PubMed: article
      - UniProt: protein
    """

    # Для логирования, метрик и роутинга
    name: str          # "chembl.target"
    source: str        # "chembl", "pubchem", "pubmed", ...

    def fetch_one(
        self,
        request: ClientRequest,
        *,
        context: RequestContext | None = None,
    ) -> Record | None:
        """
        Ожидается, что запрос содержит либо один id, либо фильтр,
        который даёт не более одной записи.
        """
        ...

    def iter_records(
        self,
        request: ClientRequest,
        *,
        context: RequestContext | None = None,
    ) -> RecordStream:
        """
        Итератор по отдельным записям с учётом PaginationParams.limit.
        """
        ...

    def iter_pages(
        self,
        request: ClientRequest,
        *,
        context: RequestContext | None = None,
    ) -> PageStream:
        """
        Итератор по страницам низкого уровня (Page),
        используется, когда важно контролировать границы страниц.
        """
        ...

    # Для использования через `with` и корректного закрытия соединений/сессий
    def close(self) -> None:
        ...

    def __enter__(self) -> "DataClient":
        ...

    def __exit__(self, exc_type, exc, tb) -> None:
        ...
