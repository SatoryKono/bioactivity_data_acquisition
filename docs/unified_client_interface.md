# Унификация клиентского интерфейса для внешних источников данных

## Сводка
Унифицированный клиент источника данных — это экземпляр, реализующий единый протокол (`DataProviderProtocol`), который:
- инкапсулирует транспорт с ретраями/таймаутами и единый контракт пагинации (`PaginationParams`),
- предоставляет согласованный набор операций (`fetch_one`, `fetch_many`, `iter_pages`, опционально `fetch_batch`/`search`),
- возвращает поток сырых записей (dict) без доменной нормализации и поднимает предсказуемый набор исключений,
- допускает конфигурирование (ключи API, таймауты, размеры страниц) единообразно для ChEMBL и остальных провайдеров.

## Текущие интерфейсы клиентов

| Группа | Класс/модуль | Методы/сигнатуры | Особенности |
| --- | --- | --- | --- |
| Фабричный слой | `clients.base.ClientFactory` | `create(entity, mode=None)` | Реестр фабрик (`chembl`, `enricher`); конфиг транспорта/пагинации не унифицирован. |
| Фабрики провайдеров | `enricher_factory.EnricherClientFactory`, `enricher_strategy_registry.StrategyRegistry` | `create(source, mode=None)`, `crossref/openalex/...()` | Собирают клиентов из конфигурации/стратегий; завязаны на `BaseEnricherClient`. |
| Низкоуровневый HTTP (провайдеры) | `enricher_base.BaseEnricherClient`, `RouteProviderBase` | `fetch_one(path, params, page_key)`, `fetch_batch(path, params, page_key, next_key, page_param)`, `call_route/ search/fetch` | Пагинация через `EnricherClientOptions`; структурированное логирование; возвращают генераторы dict. |
| Доменные клиенты провайдеров | `providers.crossref`, `providers.openalex`, `providers.pubchem`, `providers.pubmed`, `providers.semantic_scholar`, `providers.uniprot` (генерируются `create_route_provider_class`) | `fetch_one(value, params, route_name)`, `fetch_batch(value, params, route_name)` | Алиасы `fetch/search/call_route`; параметры `value` + `params`; пагинация задаётся в опциях клиента, не в сигнатурах. |
| Низкоуровневый HTTP (ChEMBL) | `chembl.adapter.ChemblTransportAdapter`, `chembl.adapter_factory.BaseChemblAdapterFactory` | `request(method, url, params, pagination_strategy)` | Оборачивают транспорт и пагинацию; логирование/метаданные в адаптере. |
| Доменные клиенты ChEMBL | `chembl.base.BaseChemblClient`, `ChemblEntityClient` | `get(id)`, `fetch_one(id, params)`, `fetch_many(page_size, params, page_key, next_key, page_param)`, `fetch_batch(ids, params, path_template)`, `iterate_records(ids, page_size, fetcher)` | Пагинация через стратегии (`PaginationStrategyResolverMixin`); есть alias-методы (`fetch_page`, `list`, `fetch_all`). Возвращают итераторы dict, частично нормализуют ответы в адаптере. |
| Фабрики ChEMBL | `chembl.entities.ChemblEntityClientFactory`, `chembl.factory.ChemblClientFactory`, `chembl.factories.default_chembl_factory` | `create(entity)`, алиасы `activity()/assay()/...` | Собирают транспорт, адаптеры и стратегии пагинации; конфигурация через `APIConfig` и resilient-executor. |
| Фасады/сервисы | `enricher_facade.EnricherFacade` | `enrich(value, client_name)` | Оборачивает вызовы клиентов, проглатывает ошибки (возвращает `None`), логирует через `structlog`. |

### Несогласованность интерфейсов
- Нейминг операций: ChEMBL использует `fetch_many/fetch_batch/get`, провайдеры — `fetch_one/fetch_batch` с `value` и `route_name`; фасад оперирует `enrich`. Есть устаревшие алиасы (`fetch`, `search`, `call_route`).
- Параметры: ChEMBL принимает `page_size` и явные `page_key/next_key/page_param`; провайдеры используют `value` + `params` и хранят настройки пагинации в `EnricherClientOptions`, а не в сигнатуре.
- Результаты: все возвращают генераторы dict, но ChEMBL часть нормализации/адаптации прячет в `ChemblTransportAdapter`, провайдеры — в `BaseEnricherClient._iterate_pages` (fallback payload и смешение HTTP/доменного слоя).
- Ошибки/логирование: провайдеры — `structlog` + `_wrap_callable` с подавлением ошибок (возврат None в фасаде); ChEMBL — ошибки транспорта/совместимости через адаптеры, контекст логов отличается.

## Целевой интерфейс

### Базовые протоколы
- `DataProviderProtocol`: единый контракт для HTTP-клиента источника данных.
  - `fetch_one(ref: str, *, params: Mapping[str, Any] | None = None, context: RequestContext | None = None) -> RecordStream`
  - `fetch_many(*, query: Mapping[str, Any] | None = None, page_size: int | None = None, pagination: PaginationParams | None = None, context: RequestContext | None = None) -> RecordStream`
  - `iter_pages(*, query: Mapping[str, Any] | None = None, pagination: PaginationParams | None = None, context: RequestContext | None = None) -> PageStream`
  - `configure(*, transport: TransportOptions | None = None, pagination: PaginationParams | None = None, retries: RetryOptions | None = None) -> Self`
  - `metadata() -> Mapping[str, Any]`, `close() -> None`.

- `PaginationParams`: dataclass с `page_key`, `next_key`, `page_param`, `page_size`; совместим с ChEMBL-стратегиями и `EnricherClientOptions`.
- `RequestContext`: источник (`source`), маршрут/сущность, дополнительные поля для логов/метрик.
- `RecordStream`/`PageStream`: итераторы словарей/страниц (JSON-compatible mappings). Страницы содержат `items`, `next_cursor` (если есть).

### Базовые реализации
- `BaseDataProvider` (миксин для транспорта + пагинации):
  - Инкапсулирует `ApiTransportProtocol` с ретраями/таймаутами и единым логированием контекста.
  - Делегирует пагинацию общей `PaginationStrategy`, принимающей `PaginationParams`.
  - Не выполняет доменной нормализации; возвращает сырой поток записей.
- Дополнительные протоколы:
  - `SupportsSearch`: `search(query: str | Mapping[str, Any], **kwargs) -> RecordStream`.
  - `SupportsBatch`: `fetch_batch(ids: Sequence[str], *, params: Mapping[str, Any] | None = None, context: RequestContext | None = None) -> RecordStream`.

### Исключения и результаты
- Единый набор исключений из `bioetl.clients.exceptions` + новые: `ProviderError` (обертка HTTP/парсинга), `PaginationError` (ошибка стратегии), `ConfigurationError` (некорректные опции).
- Результаты — итераторы словарей (`Iterator[dict[str, Any]]`); отсутствие данных не считается ошибкой, но логируется с контекстом.

## Предлагаемые изменения в коде

### Общие для всех
- Ввести модуль `clients.interfaces` с `DataProviderProtocol`, `SupportsSearch`, `SupportsBatch`, `PaginationParams`, `RequestContext`, алиасами потоков и новыми исключениями.
- Добавить `BaseDataProvider` (например, `clients.base_provider`) для общей обёртки транспорта/пагинации и стандартного логирования.
- Разделить HTTP и нормализацию: клиенты возвращают сырой JSON, нормализаторы — отдельные компоненты (`INormalizer`).

### ChEMBL
- `BaseChemblClient` реализует `DataProviderProtocol` через адаптер:
  - `get` → alias `fetch_one`, сохранить `fetch_batch`, выровнять сигнатуры `fetch_many`/`iter_pages` под `PaginationParams`.
  - Добавить `RequestContext` (source=`chembl`, entity) и прокинуть его в логирование/адаптер.
- Вынести общие части `ChemblTransportAdapter` в модуль транспорта, сделать его референсной реализацией для других источников.

### Провайдеры (Crossref/OpenAlex/PubChem и др.)
- Добавить `UnifiedProviderAdapter` для `RouteProviderBase`, реализующий `DataProviderProtocol` и переиспользующий существующие `fetch_one/fetch_batch`.
- Расширить `create_route_provider_class` генерацией `fetch_many/iter_pages` на базе `PaginationParams` и единой стратегии пагинации.
- Оставить алиасы `fetch/search/call_route` как временные, пометив deprecated и предупреждая в логах.

### Фасады/фабрики
- Обновить `EnricherClientFactory`/`ChemblClientFactory` — создавать клиентов, реализующих `DataProviderProtocol`, принимать единые опции (transport, retries, pagination).
- `EnricherFacade` переводить на `fetch_one/fetch_many/iter_pages` и использовать `RequestContext` для логирования и метрик.
- Задокументировать стабильный публичный API: `ClientFactory.create(entity, **options)` и `DataProviderProtocol`.

### Нормализация
- Оформить `INormalizer` (пример — `BaseChemblNormalizer`) и подключать нормализаторы в пайплайне/фасадах, а не в клиентах.

## План миграции и совместимость

1. **Введение интерфейсов**: добавить `clients.interfaces` + `BaseDataProvider` (без изменения внешних вызовов), выпустить адаптеры для `BaseChemblClient` и `BaseEnricherClient`.
2. **Адаптация клиентов**: реализовать `DataProviderProtocol` в ChEMBL и провайдерах через thin-адаптеры; расширить `EnricherClientOptions` до `PaginationParams`; унифицировать пагинацию на общей стратегии.
3. **Перевод фасадов/пайплайнов**: обновить `EnricherFacade`, `EnricherClientFactory`, ETL на `fetch_one/fetch_many/iter_pages` + `RequestContext`; оставить старые алиасы с предупреждениями.
4. **Нормализация и логирование**: вынести нормализацию в отдельный слой (подключение `INormalizer` в пайплайне), стандартизовать структурированное логирование/метрики в `BaseDataProvider`.
5. **Депрекация и очистка**: после миграции удалить временные адаптеры и алиасы (`call_route`, `fetch/search`), зафиксировать стабильный публичный API (`DataProviderProtocol`, `ClientFactory.create`, фабрики) и обновить README/диаграммы.

