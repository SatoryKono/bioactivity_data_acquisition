# Единый контракт клиентского слоя

## Сводка
- Все клиенты реализуют общий протокол `ExternalDataClient`, принимающий структурированный `ClientRequest` и возвращающий унифицированные типы `Record`/`Page` без доменных трансформаций.
- Общие абстракции (`RequestContext`, `Pagination`, `ClientRequest`, `Page`) расширяемы без изменения сигнатур клиентов, что снижает связность и упрощает тестирование.
- Транспортные детали (HTTP/DB), пагинация, ретраи и логирование инкапсулированы в базовой инфраструктуре; конкретные клиенты становятся тонкими конфигурационными оболочками.

## Целевой контракт клиента
```python
from __future__ import annotations
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, TypedDict, runtime_checkable

class Record(TypedDict, total=False):
    """Сырая запись внешнего источника; ключи задаются конфигурацией."""

@dataclass(slots=True)
class RequestContext:
    trace_id: str | None = None
    options: Mapping[str, Any] | None = None  # таймауты, лимиты, debug-флаги

@dataclass(slots=True)
class Pagination:
    page_size: int | None = None
    cursor: str | None = None

@dataclass(slots=True)
class ClientRequest:
    route: str  # логическое имя маршрута из YAML
    params: Mapping[str, Any] | None = None
    context: RequestContext | None = None
    pagination: Pagination | None = None

@dataclass(slots=True)
class Page:
    items: list[Record]
    next_cursor: str | None = None
    raw: Any | None = None  # исходный ответ транспорта

@runtime_checkable
class ExternalDataClient(Protocol):
    def fetch_one(self, request: ClientRequest) -> Record | None: ...
    def fetch_many(self, request: ClientRequest) -> Iterator[Record]: ...
    def iter_pages(self, request: ClientRequest) -> Iterator[Page]: ...
    def metadata(self) -> Mapping[str, Any]: ...  # лимиты API, user-agent
    def close(self) -> None: ...
```
Ключевые свойства:
- Один протокол для всех источников: одинаковые сигнатуры и возвращаемые типы.
- Запрос описывается `ClientRequest`, а расширяемый `RequestContext.options` позволяет добавлять таймауты/trace-id без изменения интерфейса.
- Пагинация управляется общими компонентами транспорта, клиенты не дублируют курсоры/offset.

## Сопоставление текущих клиентов с контрактом
| Источник | Текущий класс(ы) | Методы сейчас | Соответствие целевому контракту | Что нужно изменить/удалить |
| --- | --- | --- | --- | --- |
| ChEMBL | `chembl.data_client.ChemblDataClient`, `chembl.client.ChemblClient` | `fetch`, `search`, легаси-миксины | Нет `ClientRequest`, есть легаси-совместимость | Удалить совместимость-слой, реализовать `ExternalDataClient`, маршруты и пагинацию описать в YAML |
| PubChem | `pubchem.client.PubChemDataClientImpl` | `fetch`, `search`, alias-методы с DeprecationWarnings | Сигнатуры и нормализация отличаются | Заменить на `fetch_one`/`fetch_many`/`iter_pages`, убрать нормализацию и варнинги, вынести параметры в YAML |
| PubMed | `pubmed.client.PubMedDataClientImpl` | `efetch`, `esearch`, XML-нормализация | Нет `ClientRequest`, смешение логики | Оставить только тонкий вызов транспорта; XML разбор перенести в пайплайн |
| OpenAlex | `openalex.client.OpenAlexDataClientImpl` | `_iterate_pages_impl`, `search`, алиасы | Пагинация и нормализация внутри | Переписать на единый контракт, пагинацию конфигурировать, удалить алиасы |
| Crossref | `crossref.client.CrossrefDataClientImpl` | Специальная стратегия пагинации | Своя стратегия и нормализация | Использовать общую пагинацию по `Pagination`, убрать bespoke слой |
| Semantic Scholar | `semantic_scholar.client.SemanticScholarDataClientImpl` | Поисковые методы с нормализацией | Сигнатуры отличаются, есть доменная логика | Вынести нормализацию, оставить тонкий клиент по `ClientRequest` |
| UniProt | `uniprot.client.UniProtDataClientImpl` | Поиск/детализация + нормализация | Нет единого запроса/страниц | Перейти на новый контракт, нормализацию вынести |
| IUPHAR Target | `providers/iuphar` + фабрики | Специальные фабрики и нормализация | Не соответствует | Заменить фабрику на общий, перенести доменную логику |

## Пример реализации тонкого клиента
```python
# src/bioetl/clients/base/client.py
from bioetl.clients.base.contracts import (
    ClientRequest, ExternalDataClient, Page, Record
)
from bioetl.clients.base.transport import Transport

class BaseExternalDataClient(ExternalDataClient):
    """Базовый клиент, делегирующий все вызовы транспорту и маршрутам."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def fetch_one(self, request: ClientRequest) -> Record | None:
        page = next(self.iter_pages(request), None)
        return page.items[0] if page and page.items else None

    def fetch_many(self, request: ClientRequest):
        for page in self.iter_pages(request):
            yield from page.items

    def iter_pages(self, request: ClientRequest):
        yield from self._transport.execute(request)

    def metadata(self):
        return self._transport.metadata()

    def close(self) -> None:
        self._transport.close()
```
```python
# src/bioetl/clients/chembl/client.py
from bioetl.clients.base.client import BaseExternalDataClient
from bioetl.clients.config.loader import load_source_config
from bioetl.clients.base.transport import http_transport_factory

class ChemblClient(BaseExternalDataClient):
    def __init__(self, config_path: str) -> None:
        config = load_source_config(config_path)
        super().__init__(transport=http_transport_factory(config))

# Использование
client = ChemblClient("chembl")
request = ClientRequest(route="fetch_molecule", params={"chembl_id": "CHEMBL25"})
record = client.fetch_one(request)
```
Конкретный клиент только загружает конфигурацию и создаёт транспорт; никакой доменной логики внутри.

## Запрещённая логика в клиентах
- **Доменные трансформации и нормализация**: выносить в пайплайны/сервисы обработки ответов (например, модули трансформации ETL).
- **Агрегации, обогащения, дополнительный поиск**: выполнять в отдельных стадиях ETL или доменных сервисах, а не внутри клиента.
- **Валидация данных по бизнес-правилам**: применять после извлечения (схемы Pandera, QC-отчёты), а не при вызове клиента.
- **Управление ретраями, логированием, трассировкой**: инкапсулировать в общем транспорте; клиент только прокидывает `RequestContext`.
- **Слои совместимости и алиасы старых интерфейсов**: удалить; каждая реализация напрямую имплементирует `ExternalDataClient` без адаптеров.
