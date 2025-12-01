# Унификация клиентского интерфейса для внешних источников данных

## Сводка
Унифицированный клиент источника данных — экземпляр, реализующий общий протокол (`DataProviderProtocol`), который:
- инкапсулирует транспорт с таймаутами/ретраями и единый контракт пагинации (`PaginationParams`);
- предоставляет согласованный набор операций (`fetch_one`, `fetch_many`, `iter_pages`, опционально `fetch_batch`/`search`);
- возвращает поток сырых записей (dict) без доменной нормализации и поднимает предсказуемый набор исключений (`ProviderError`, `PaginationError`, `ConfigurationError` и re-export из `requests`);
- конфигурируется единообразно (ключи API, таймауты, размеры страниц) для ChEMBL и остальных провайдеров.

## Текущие интерфейсы клиентов

| Группа | Класс/модуль | Методы/сигнатуры | Особенности интерфейса |
| --- | --- | --- | --- |
| ChEMBL — фабрики/транспорт | `clients.chembl.factories.default_chembl_factory`, `chembl.adapter.ChemblTransportAdapter`, `chembl.adapter_factory.BaseChemblAdapterFactory` | `default_chembl_factory(config, pagination_strategy, transport_factory)`, `request(method, url, params, pagination_strategy)` | Богатая сборка транспорта + адаптер с логированием, resilient executor, кастомные стратегии пагинации. |
| ChEMBL — доменные клиенты | `chembl.base.BaseChemblClient`, `ChemblEntityClient`, `chembl.entities.ChemblEntityClientFactory` | `get(id)`, `fetch_one(id, params)`, `fetch_many(page_size, params, page_key, next_key, page_param)`, `fetch_batch(ids, params, path_template)`, `iterate_records(ids, page_size, fetcher)`, alias `fetch_page/list/fetch_all` | Явные параметры пагинации в сигнатурах; итераторы dict; частичная нормализация в адаптере; алиасы и устаревшие методы. |
| Провайдеры — фабрики/фасады | `enricher_factory.EnricherClientFactory`, `enricher_strategy_registry.StrategyRegistry`, `enricher_facade.EnricherFacade` | `create(source, mode=None)`, `from_config`, `enrich(value, client_name)` | Привязаны к `BaseEnricherClient`; фасад подавляет ошибки (возвращает `None`) и логирует через `structlog`. |
| Провайдеры — низкоуровневый HTTP | `enricher_base.BaseEnricherClient`, `RouteProviderBase` | `fetch_one(path, params, page_key)`, `fetch_batch(path, params, page_key, next_key, page_param)`, алиасы `fetch/search/call_route` | Параметры пагинации в `EnricherClientOptions`, а не в сигнатуре; генераторы dict; общий `_wrap_callable` для логирования/обработки ошибок. |
| Провайдеры — доменные клиенты | `providers.crossref`, `providers.openalex`, `providers.pubchem`, `providers.pubmed`, `providers.semantic_scholar`, `providers.uniprot` (генерируются `create_route_provider_class`) | `fetch_one(value, params, route_name)`, `fetch_batch(value, params, route_name)` | Алиасы и маршруты задаются декларативно; параметры `value` + `params`; пагинация конфигурируется при создании. |
| Общий фабричный слой | `clients.base.ClientFactory` | `create(entity, mode=None)` | Реестр фабрик (`chembl`, `enricher`); конфигурация транспорта/пагинации разная между группами. |

### Несогласованность интерфейсов
- Нейминг операций: ChEMBL — `fetch_many/fetch_batch/get`, провайдеры — `fetch_one/fetch_batch` + `route_name`, фасад — `enrich`; присутствуют устаревшие алиасы (`fetch`, `search`, `call_route`).
- Параметры: ChEMBL принимает `page_size` и явные `page_key/next_key/page_param`; провайдеры используют `value` + `params` и хранят настройки пагинации в `EnricherClientOptions`, а не в сигнатуре.
- Результаты: обе группы возвращают генераторы dict, но ChEMBL выполняет часть нормализации в адаптере (`ChemblTransportAdapter`), а провайдеры — в `_iterate_pages` (смешение HTTP/доменной логики).
- Ошибки/логирование: провайдеры логируют через `structlog` и часто глушат исключения на уровне фасада; ChEMBL поднимает исключения транспорта/адаптера с иным контекстом.

## Целевой интерфейс

### Базовые протоколы
- `DataProviderProtocol`: единый контракт клиента источника данных.
  - `fetch_one(ref: str, *, params: Mapping[str, Any] | None = None, context: RequestContext | None = None) -> RecordStream`
  - `fetch_many(*, query: Mapping[str, Any] | None = None, page_size: int | None = None, pagination: PaginationParams | None = None, context: RequestContext | None = None) -> RecordStream`
  - `iter_pages(*, query: Mapping[str, Any] | None = None, pagination: PaginationParams | None = None, context: RequestContext | None = None) -> PageStream`
  - `configure(*, transport: TransportOptions | None = None, pagination: PaginationParams | None = None, retries: RetryOptions | None = None) -> Self`
  - `metadata() -> Mapping[str, Any]`, `close() -> None`.
- `PaginationParams`: dataclass (`page_key`, `next_key`, `page_param`, `page_size`), совместимая с ChEMBL-стратегиями и `EnricherClientOptions`.
- `RequestContext`: содержит источник (`source`), маршрут/сущность и дополнительные поля для логов/метрик.
- `RecordStream`/`PageStream`: итераторы словарей/страниц (JSON-compatible mappings); страница несёт `items` и `next_cursor` (если есть).

### Базовые реализации
- `BaseDataProvider` (транспорт + пагинация): инкапсулирует `ApiTransportProtocol` с ретраями/таймаутами и единым логированием контекста, делегирует пагинацию общей `PaginationStrategy`, не выполняет доменной нормализации.
- Дополнительные протоколы: `SupportsSearch` (`search(query, **kwargs)`), `SupportsBatch` (`fetch_batch(ids, *, params=None, context=None)`).

### Исключения и результаты
- Единый набор исключений: существующие из `bioetl.clients.exceptions` + `ProviderError`, `PaginationError`, `ConfigurationError`.
- Результаты — итераторы словарей; отсутствие данных логируется, но не считается ошибкой.

## Предлагаемые изменения в коде

### Общие для всех источников
- Ввести модуль `clients.interfaces` с `DataProviderProtocol`, `SupportsSearch`, `SupportsBatch`, `PaginationParams`, `RequestContext`, потоковыми алиасами и новыми исключениями.
- Добавить `BaseDataProvider` (например, `clients.base_provider`) как общую обёртку транспорта/пагинации и стандартное структурированное логирование; отделить HTTP от нормализации (`INormalizer`).

### ChEMBL
- Сделать `BaseChemblClient` реализацией `DataProviderProtocol`: `get` → алиас `fetch_one`; выровнять `fetch_many/iter_pages` под `PaginationParams`; сохранить `fetch_batch` с теми же именами аргументов, добавив поддержку `RequestContext` (source=`chembl`, entity).
- Вынести общие части `ChemblTransportAdapter` в референсный транспортный модуль (логирование, ретраи, проверка ответов) и подключить его к `BaseDataProvider`.

### Провайдеры (Crossref, OpenAlex, PubChem, PubMed, Semantic Scholar, UniProt)
- Добавить `UnifiedProviderAdapter` поверх `RouteProviderBase`, реализующий `DataProviderProtocol` через существующие `fetch_one/fetch_batch`.
- Расширить `create_route_provider_class` генерацией `fetch_many/iter_pages` на базе `PaginationParams` и общей стратегии пагинации.
- Алиасы `fetch/search/call_route` оставить временно через `DeprecatedAliasMixin`, логируя предупреждения.

### Фабрики и фасады
- Обновить `EnricherClientFactory`/`ChemblClientFactory` так, чтобы все выдаваемые клиенты реализовывали `DataProviderProtocol`, принимали единые опции транспорта/ретраев/пагинации.
- Перевести `EnricherFacade` на вызовы `fetch_one/fetch_many/iter_pages` с `RequestContext` для унифицированного логирования/метрик.
- Задокументировать стабильный публичный API: `ClientFactory.create(entity, **options)` и `DataProviderProtocol`.

### Нормализация
- Ввести `INormalizer` (референс — `BaseChemblNormalizer`) и подключать нормализацию в пайплайнах/фасадах, оставляя клиентов источников ответственными только за получение сырых данных.

## План миграции и совместимость

1. **Введение интерфейсов**: добавить `clients.interfaces` + `BaseDataProvider` без изменения существующих вызовов; подготовить совместимые исключения и `PaginationParams`/`RequestContext`.
2. **Адаптация клиентов**: подключить thin-адаптеры, чтобы `BaseChemblClient` и `BaseEnricherClient` удовлетворяли `DataProviderProtocol`; расширить `EnricherClientOptions` до `PaginationParams`; унифицировать пагинацию на общей стратегии.
3. **Перевод фасадов/пайплайнов**: обновить `EnricherFacade`, `EnricherClientFactory`, ETL на новые методы (`fetch_one/fetch_many/iter_pages`) и контекст; сохранить старые алиасы с предупреждениями в логах.
4. **Нормализация и логирование**: вынести нормализацию в отдельный слой (`INormalizer`), стандартизовать структурированное логирование и метрики внутри `BaseDataProvider`/адаптеров.
5. **Депрекация и очистка**: удалить временные адаптеры/алиасы после перевода потребителей, закрепить стабильный API (`DataProviderProtocol`, фабрики), обновить README/диаграммы и указать поддерживаемые публичные методы.
