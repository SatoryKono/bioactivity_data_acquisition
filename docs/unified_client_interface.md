# Унификация клиентского интерфейса для внешних источников данных

## Сводка
Единый клиент источника данных должен предоставлять согласованный минимальный контракт для HTTP-доступа к внешним источникам (ChEMBL и провайдеры). Базовая функциональность: конфигурируемый транспорт с ретраями/таймаутами, единообразная пагинация, потоковая выдача нормализованных записей и предсказуемые исключения. Клиент не смешивает сетевую логику с доменной нормализацией, оставляя последнюю для отдельного слоя.

## Текущие интерфейсы клиентов

| Группа | Класс/модуль | Методы/сигнатуры | Особенности |
| --- | --- | --- | --- |
| Фабричный слой | `clients.base.ClientFactory` | `create(entity, mode=None)` | Реестр фабрик по доменным алиасам (`chembl`, `enricher`); фактическая сигнатура без унифицированных опций транспорта/пагинации. |
| Провайдеры (enrichers) | `enricher_base.BaseEnricherClient` / `RouteProviderMixin` | `fetch_one(path, params, page_key)`, `fetch_batch(path, params, page_key, next_key, page_param)`, `search/fetch` через `RouteConfig` | Параметры пагинации в опциях (`EnricherClientOptions`), логирование через `structlog`; результаты — генераторы словарей; маршруты задаются декларативно. |
| Провайдеры — конкретные | `providers.crossref.OpenAlex…` (генерируются через `create_route_provider_class`) | `fetch_one(value, params, route_name)`, `fetch_batch(value, params, route_name)` | Алиасы устаревших методов (`fetch`/`search`), аргументы `value` + `params`, пагинация задаётся опциями клиента. |
| ChEMBL — базовый | `chembl.base.BaseChemblClient` | `get(id)`, `fetch_one(id, params)`, `fetch_many(page_size, params, page_key, next_key, page_param)`, `fetch_batch(ids, params, path_template)`, `iterate_records(ids, page_size, fetcher)` | Интегрирован с `PaginationStrategyResolverMixin` и `ChemblTransportAdapter`; возвращает итераторы словарей, есть alias-методы (`fetch_page`, `list`, `fetch_all`). |
| ChEMBL — фабрики | `chembl.entities.ChemblEntityClientFactory`, `chembl.factory.ChemblClientFactory` | `create(entity)`, алиасы `activity()/assay()/...` | Собирают транспорт, адаптеры, стратегии пагинации; тесно связаны с конфигом `default_chembl_factory`. |
| Фасады/оркестраторы | `enricher_facade.EnricherFacade`, `enricher_factory.EnricherClientFactory` | `enrich(value, client_name)`, фабричные методы `crossref/openalex/...` | Прячут различия клиентов, но сигнатуры и поведения остаются разнородными; ошибки логируются и проглатываются (`None` на неудачу). |

### Несогласованность интерфейсов
- Нейминг операций: ChEMBL использует `fetch_many/fetch_batch/get`, провайдеры — `fetch_one/fetch_batch` с `value` и `route_name`; устаревшие алиасы (`fetch`, `search`, `call_route`).
- Параметры: ChEMBL принимает `page_size` и строгий набор `page_key/next_key/page_param`; провайдеры используют `value` + `params` и опции по умолчанию в `EnricherClientOptions`.
- Результаты: везде генераторы словарей, но ChEMBL часть логики нормализации/адаптации вынесена в `ChemblTransportAdapter`, а в провайдерах — внутри `BaseEnricherClient._iterate_pages` (вставки `fallback_payload`).
- Ошибки/логирование: провайдеры логируют через `structlog` и оборачивают вызовы `_wrap_callable`; ChEMBL полагается на `ChemblCompatibilityMixin` и транспортные адаптеры, лог контекста формируется по-другому.

## Целевой интерфейс

### Базовые протоколы
- `DataProviderProtocol`: единый контракт для HTTP-клиента источника данных.
  - `fetch_one(ref: str, *, params: Mapping[str, Any] | None = None, context: RequestContext | None = None) -> RecordStream`
  - `fetch_many(*, query: Mapping[str, Any] | None = None, page_size: int | None = None, pagination: PaginationParams | None = None, context: RequestContext | None = None) -> RecordStream`
  - `iter_pages(*, query: Mapping[str, Any] | None = None, pagination: PaginationParams | None = None, context: RequestContext | None = None) -> PageStream`
  - `configure(*, transport: TransportOptions | None = None, pagination: PaginationParams | None = None, retries: RetryOptions | None = None) -> Self` (флюентный стиль или фабрика-строитель).
  - `metadata() -> Mapping[str, Any]`, `close() -> None`.

- `PaginationParams`: dataclass с `page_key`, `next_key`, `page_param`, `page_size`. Совместим с существующими стратегиями ChEMBL и опциями enricher.
- `RequestContext`: источник (`source`), маршрут/сущность, дополнительные поля для логов/метрик.
- `RecordStream`/`PageStream`: типы-алиасы генераторов словарей/страниц (JSON-compatible mappings).

### Базовые реализации
- `BaseDataProvider` (наследует `ClosableMixin`, `ApiClientMixin`):
  - Инкапсулирует обёртку транспорта (`ApiTransportProtocol`) с ретраями/таймаутами.
  - Делегирует пагинацию в единую `PaginationStrategy` (совместимую с обоими семействами клиентов).
  - Не выполняет доменной нормализации; возвращает сырой поток записей.
- Дополнительные протоколы (опционально):
  - `SupportsSearch`: добавляет `search(query: str | Mapping[str, Any], **kwargs)`.
  - `SupportsBatch`: добавляет `fetch_batch(ids: Sequence[str], **kwargs)`.

### Исключения и результаты
- Единый набор исключений из `bioetl.clients.exceptions` + собственный `ProviderError` (HTTP/парсинг), `PaginationError` (ошибка стратегии) и `ConfigurationError` (некорректные опции).
- Результаты — итераторы словарей (`Iterator[dict[str, Any]]`); отсутствие данных не считается ошибкой, но логируется с контекстом.

## Предлагаемые изменения в коде

### Общие для всех
- Ввести новый модуль `clients.interfaces` с `DataProviderProtocol`, `SupportsSearch`, `SupportsBatch`, `PaginationParams`, `RequestContext` и алиасами потоков.
- Добавить `BaseDataProvider` в `clients.common` или новый модуль `clients.base_provider`, реализующий общие обёртки транспорта/пагинации и логирование.
- Уточнить и переиспользовать единый набор исключений (расширить `clients.exceptions`).

### ChEMBL
- `BaseChemblClient` реализует `DataProviderProtocol`: 
  - Переименовать/алиасировать `fetch_many` -> `fetch_many` (совместимо), `fetch_batch` сохранить; `get` предоставить как `fetch_one` (adapter-метод). 
  - Добавить поддержку `RequestContext` (entity, source=`chembl`) и общих `PaginationParams` поверх существующих `PaginationStrategyResolverMixin`.
- `ChemblTransportAdapter` может стать эталонной реализацией транспорта/пагинации для других клиентов; вынести части в общий модуль, чтобы провайдеры могли переиспользовать.

### Провайдеры (Crossref/OpenAlex/PubChem и др.)
- Обернуть существующие `RouteProviderBase` адаптером `UnifiedProviderAdapter`, реализующим `DataProviderProtocol` и перенаправляющим на `fetch_one/fetch_batch` текущих классов.
- В `create_route_provider_class` добавить возможность автоматической генерации методов `fetch_many/iter_pages` с `PaginationParams`.
- Удалить устаревшие алиасы (`fetch`, `search`, `call_route`) после перехода потребителей; временно оставить через `DeprecatedAliasMixin` с явным предупреждением.

### Фасады/фабрики
- `EnricherClientFactory` и `ChemblClientFactory` должны возвращать объекты, реализующие `DataProviderProtocol`; добавить единые параметры конфигурации транспорта/пагинации/ретраев.
- `EnricherFacade` переводить на вызовы `fetch_one/fetch_many` нового протокола и логировать через общий контекст.
- Документировать, что `ClientFactory.create(entity, **options)` — стабильный API; `mode` оставить для обратной совместимости как alias к опциям.

### Нормализация
- Вынести `BaseChemblNormalizer` как пример `INormalizer` и позволить провайдерам подключать собственные нормализаторы через отдельный слой (вне клиента). Клиенты возвращают сырой JSON.

## План миграции и совместимость

1. **Введение интерфейсов**: добавить `clients.interfaces` и `BaseDataProvider`, не меняя существующий код; обеспечить адаптер для `BaseChemblClient` и `BaseEnricherClient` без изменения вызовов.
2. **Адаптация клиентов**: реализовать `DataProviderProtocol` в ChEMBL и провайдерах через thin-адаптеры; расширить `EnricherClientOptions` до `PaginationParams` и использовать общую стратегию пагинации.
3. **Перевод фасадов/пайплайнов**: обновить `EnricherFacade`, `EnricherClientFactory`, ETL-пайплайны на новый контракт (`fetch_one/fetch_many/iter_pages`, `RequestContext`); оставить устаревшие методы с предупреждениями.
4. **Нормализация и логирование**: вынести нормализацию из клиентов в отдельные шаги ETL; стандартизовать структурированное логирование в базовом провайдере.
5. **Депрекация и очистка**: после перевода потребителей убрать временные адаптеры и устаревшие алиасы (`call_route`, `fetch/search`), задокументировать стабильный публичный API (`DataProviderProtocol`, `ClientFactory.create`, базовые фабрики). Обновить README/диаграммы.

