## Сводка
- Унифицируем две линии клиентов: ChEMBL-ориентированные сущностные клиенты и набор route-провайдеров (PubChem, PubMed, OpenAlex, Crossref, Semantic Scholar, UniProt).
- В качестве базового контракта принимаем `DataProviderProtocol` с методами `fetch_one`, `iter_pages`, `fetch_many`, `metadata`, `close`, а также опцией `configure` для транспорта/пагинации/ретраев. Источником правды остаются `DataProviderProtocol` и `BaseDataProvider`.

## Текущие интерфейсы
| Группа | Класс/модуль | Методы/сигнатуры | Особенности |
| --- | --- | --- | --- |
| ChEMBL (доменные клиенты) | `BaseChemblClient` (`clients/chembl/base.py`) | `fetch_one(ref, params)`, `fetch_many(page_size=1000, params, page_key, next_key, page_param)`, `fetch_batch(ids, params, path_template)`, `search(params)`, `status()`, алиасы `list/fetch_page/fetch_all` и др. | Пагинация через стратегии, дефолтные ключи page/next, поддержка `configure(pagination)`; возвращает итераторы словарей, метаданные транспорта через `metadata` проперти. 【F:src/bioetl/clients/chembl/base.py†L33-L206】 |
| Transport-ориентированный базовый провайдер | `BaseDataProvider` (`clients/providers/base_provider.py`) | Реализует `fetch_one(ref, params, context) -> RecordStream`, `iter_pages(query, pagination, context) -> PageStream`, `fetch_many(query, page_size, pagination, context) -> RecordStream`, `configure(transport, pagination, retries)` | Использует `ApiTransportProtocol`, нормализует payload через `normalize_payload`, поддерживает limit из `page_size`, возвращает `Page` с `items/next_cursor/raw`. 【F:src/bioetl/clients/providers/base_provider.py†L1-L194】 |
| Route-провайдеры (PubChem, PubMed, OpenAlex, Crossref, Semantic Scholar, UniProt) | Динамически генерируются через `create_route_provider_class` (например, `OpenAlexClient`) | Базовые методы `fetch_one(value, params, route_name, page_key)` и `fetch_batch(value, params, route_name, page_key, next_key, page_param)` приходят из `RouteProviderBase`; маршруты задаются через `RouteConfig` (`fetch` и `search`) | Работа с HTTP через `RouteProviderBase`/`BaseEnricherClient`, пагинация по JSON-ключам (`page_key/next_key/page_param`), поддержка устаревших алиасов (`fetch` → `fetch_one`, `search` → `fetch_batch`). 【F:src/bioetl/clients/enricher_base.py†L251-L350】【F:src/bioetl/clients/providers/openalex.py†L1-L17】 |
| Унификация route-клиентов | `UnifiedProviderAdapter` (`clients/enricher_base.py`) | Реализует `fetch_one`, `iter_pages`, `fetch_many`, `metadata`, `close`, `configure(pagination)` поверх `RouteProviderBase` | Перенастраивает `PaginationParams`, извлекает `value` из query, нормализует страницы к `Page(items,next_cursor,raw)`; поддерживает `RequestContext.route`. 【F:src/bioetl/clients/enricher_base.py†L251-L375】 |

## Целевой интерфейс
- **Протокол**: `DataProviderProtocol` остаётся единым контрактом: `fetch_one(ref, params=None, context=None)`, `iter_pages(query=None, pagination=None, context=None)`, `fetch_many(query=None, page_size=None, pagination=None, context=None)`, `metadata()`, `close()`, `configure(transport=None, pagination=None, retries=None)`.
- **Контекстные типы**: `RequestContext` хранит `source`, `route`, `extra` для логирования/метрик; `PaginationParams` описывает `page_key/next_key/page_param/page_size` и умеет `override`; `Page` и `PageStream` фиксируют структуру страницы (items + next_cursor + raw). 【F:src/bioetl/clients/base/interfaces.py†L19-L167】
- **Alias EnricherClientProtocol**: совпадает с `DataProviderProtocol` по минимальному набору методов; дополнительные протоколы `SupportsSearch`/`SupportsBatch` остаются расширениями, но не меняют ядро.
- **Типы и алиасы**: в общем модуле типов (`clients/base/interfaces.py`) закрепляем `RecordStream=Iterator[dict[str,Any]]`, `PageStream=Iterator[Page]`, `PaginationParams`, `RequestContext`, `TransportOptions`, `RetryOptions`.

## UnifiedProviderAdapter
- **Назначение**: привести любые `RouteProviderBase` или совместимые клиенты к `DataProviderProtocol` без изменения верхних слоёв.
- **Route-провайдер**: 
  - `configure` — обновляет `PaginationParams` адаптера; транспорт/ретраи прокидываются во внутренний провайдер отдельно.
  - `fetch_one` — вызывает `provider.fetch_one(ref, route_name=context.route, page_key=...)`.
  - `iter_pages` — берёт `value` из `query["value"]`, склеивает путь через `_resolve_route`, добавляет `limit` из `page_size`, выполняет `api_client.paginate_json` с ключами `page_key/next_key/page_param`, нормализует payload через `_normalize_payload`, возвращает `Page(items,next_cursor,raw)`.
  - `fetch_many` — просто разворачивает `iter_pages` с учётом `page_size`.
  - `metadata/close` — проксируют к внутреннему провайдеру/транспорту. 【F:src/bioetl/clients/enricher_base.py†L251-L375】
- **Доменный клиент (ChEMBL) через адаптер**: минимальная обёртка может проксировать вызовы к уже совместимым `fetch_one/fetch_many/fetch_batch` `BaseChemblClient`, лишь приводя параметры пагинации к `PaginationParams` и возвращая `Page` при обходе страниц (например, упаковка результатов `fetch_many` в `Page(items, next_cursor=None)`).
- **Шаблон класса** (эскиз):
  ```python
  class UnifiedProviderAdapter(DataProviderProtocol[dict[str, Any]]):
      def __init__(self, provider: Any, *, pagination: PaginationParams | None = None):
          self._provider = provider
          self._pagination = pagination or PaginationParams(
              page_key=getattr(provider, "page_key", None),
              next_key=getattr(provider, "next_key", None),
              page_param=getattr(provider, "page_param", None),
          )
      # методы fetch_one/iter_pages/fetch_many/metadata/close — прокси + нормализация
  ```

## Маппинг провайдеров
| Провайдер | Текущий класс/метод | Соответствие целевому методу | Требуемые адаптации |
| --- | --- | --- | --- |
| ChEMBL сущности | `BaseChemblClient.fetch_one/fetch_many/fetch_batch/search/status` | `fetch_one` и `fetch_many` уже совпадают; `fetch_batch` можно экспортировать через `SupportsBatch`; `metadata` через проперти | Привести пагинацию к `PaginationParams`, возвращать `Page` при постраничном обходе (для `iter_pages`), поддержать `RequestContext` (source/route) в логах. 【F:src/bioetl/clients/chembl/base.py†L33-L206】 |
| PubChem | `PubChemClient.fetch_one/fetch_batch` (маршруты `/compound/{value}`, `/compound/search`) | `fetch_one`, `fetch_many` (через `fetch_batch` + `UnifiedProviderAdapter.iter_pages`) | Требуется обёртка `UnifiedProviderAdapter` + обязательное поле `query['value']` для поиска/получения; унификация `page_key/next_key`. 【F:src/bioetl/clients/enricher_base.py†L251-L375】 |
| PubMed | `PubmedClient.fetch_one/fetch_batch` (маршруты `/pubmed/{value}`, `/pubmed`) | То же, что выше | Аналогично, адаптер обеспечивает единый контракт, алиасы `search_by_title/fetch_by_pmid` становятся устаревшими. 【F:src/bioetl/clients/providers/pubmed.py†L1-L16】 |
| OpenAlex | `OpenAlexClient.fetch_one/fetch_batch` (`/works/{value}`, `/works?search=`) | Совпадает через адаптер | Нормализовать пагинацию/ключи через `PaginationParams`, прокидывать `route` из контекста. 【F:src/bioetl/clients/providers/openalex.py†L1-L17】 |
| Crossref | `CrossrefClient.fetch_one/fetch_batch` (`/works/{value}`, `/works?query=`) | Совпадает через адаптер | Аналогично OpenAlex. 【F:src/bioetl/clients/providers/crossref.py†L1-L16】 |
| Semantic Scholar | `SemanticScholarClient.fetch_one/fetch_batch` (`/paper/{value}`, `/paper/search`) | Совпадает через адаптер | Нужно нормализовать `title_search` алиас; `next_key`/`page_key` задаются адаптером. 【F:src/bioetl/clients/providers/semantic_scholar.py†L1-L16】 |
| UniProt | `UniProtClient.fetch_one/fetch_batch` (`/uniprot/{value}`, `/uniprot/search`) | Совпадает через адаптер | Аналогично, через `UnifiedProviderAdapter` и единые ключи пагинации. 【F:src/bioetl/clients/providers/uniprot.py†L1-L14】 |
| Transportный слой | `BaseDataProvider` | Уже реализует полный контракт | Используется как эталон сигнатур для остальных классов. 【F:src/bioetl/clients/providers/base_provider.py†L36-L194】 |

## План миграции и совместимость
1. **Формализация контракта**: оставить `DataProviderProtocol`, `RequestContext`, `PaginationParams`, `Page/PageStream` в качестве единственного источника правды; при необходимости задокументировать минимальные требования к `RecordStream/PageStream` и контексту.
2. **Уточнение базового класса**: проверить, что `BaseDataProvider` покрывает целевые сигнатуры; расширить документацию/типизацию, пометить устаревшие алиасы в ChEMBL и route-клиентах.
3. **Единый адаптер**: утвердить `UnifiedProviderAdapter` как стандарт для route-провайдеров и ввести тонкую вариацию для ChEMBL (если понадобится упаковка в `Page`).
4. **Оборачивание провайдеров**: подключить адаптер в местах использования (пайплайны/сервисы), постепенно заменяя прямые вызовы `fetch_batch/search` на `fetch_many/iter_pages`.
5. **Обратная совместимость**: оставить устаревшие методы (`fetch`, `search`, `list`, `fetch_page`, `title_search`) с DeprecationWarning минимум на один минорный релиз; предоставить реэкспорт `UnifiedProviderAdapter` рядом с существующими фабриками.
6. **Удаление старого API**: после миграции удалить устаревшие алиасы и прямые обращения к внутренним маршрутам, оставив только `DataProviderProtocol`/`BaseDataProvider`/`UnifiedProviderAdapter`.
