## Сводка
Единый клиент источника данных рассматривается как абстракция поверх HTTP-провайдеров, которая предоставляет согласованные методы извлечения (`fetch_one`, `fetch_many`, `iter_pages`) с единым контрактом по результатам (итерируемые словари) и ошибкам (исключения `DataProviderError` и её подтипы). Интерфейс должен позволять конфигурировать таймауты, ретраи, параметры пагинации и аутентификации, а также поддерживать как прямые операции получения по идентификатору, так и поисковые запросы/батчи.

## Текущие интерфейсы клиентов
| Группа | Класс/модуль | Методы/сигнатуры | Особенности |
| --- | --- | --- | --- |
| ChEMBL – доменные клиенты | `BaseChemblClient`, `ChemblEntityClient` (`clients/chembl/base.py`) | Богатый протокол: `get`, `fetch_one(id)`, `fetch_many(page_size, params, page_key, next_key, page_param)`, `fetch_batch(ids, path_template)`, `iterate_records(ids, page_size, fetcher)`, `search(params)`, `status`, алиасы `fetch_page/list/fetch_all/fetch_by_ids` | Работает поверх `BaseApiEntityClient`, возвращает итераторы `dict`, поддерживает выбор стратегии пагинации и адаптер транспорта; множество устаревших алиасов и специфичные параметры по умолчанию для ChEMBL (page_size=1000, ключи `page`/`next`).【F:src/bioetl/clients/chembl/base.py†L25-L198】 |
| Общая фабрика клиентов | `ClientFactory` / `register_factory` (`clients/base.py`) | `create(entity, mode=None)` фабрик; регистрация/получение по доменному имени | Только фабричный уровень, без описания операций клиента; используется для связки домена и конкретной реализации, важен для обратной совместимости CLI/пайплайнов.【F:src/bioetl/clients/base.py†L17-L57】 |
| Обогащающие провайдеры (Crossref, OpenAlex, PubChem, PubMed, SemanticScholar, UniProt) | Сгенерированные классы `*Client` через `create_route_provider_class` (`clients/providers/*.py`) | Базовые методы `fetch_one(value, route_name?, page_key?)`, `fetch_batch(value, route_name?, page_key?, next_key?, page_param?)`, плюс устаревшие алиасы (`fetch`/`search` и др.) | Работают через `RouteProviderBase`: маршрутизация путей/параметров, опции пагинации (`page_key/next_key/page_param`), логирование `api_call`, возврат `JSONRecordStream` (итератор `dict`), устаревшие алиасы проксируются с `DeprecationWarning`.【F:src/bioetl/clients/enricher_base.py†L140-L519】【F:src/bioetl/clients/providers/crossref.py†L1-L17】【F:src/bioetl/clients/providers/openalex.py†L1-L17】【F:src/bioetl/clients/providers/pubchem.py†L1-L24】【F:src/bioetl/clients/providers/pubmed.py†L1-L22】【F:src/bioetl/clients/providers/semantic_scholar.py†L1-L21】【F:src/bioetl/clients/providers/uniprot.py†L1-L17】 |
| Обогащающие базовые утилиты | `BaseEnricherClient`, `RouteProviderMixin`, `DeprecatedAliasMixin` (`clients/enricher_base.py`) | Обёртка над `BaseApiClient`: `fetch_one(path, params, page_key)`, `fetch_batch(path, params, page_key, next_key, page_param)`, `_iterate_pages` c логированием и fallback, опции таймаутов/ретраев, нормализация списков/страниц | Результаты — итераторы словарей; возвращает fallback при пустых страницах; логирование `api_call`; алиасы позволяют плавную миграцию. Параметры пагинации отличаются по умолчанию от ChEMBL (`results/next/page`).【F:src/bioetl/clients/enricher_base.py†L23-L319】 |

## Целевой интерфейс
**Базовые протоколы**
- `DataProvider` (Protocol):
  - `fetch_one(identifier: str | Mapping[str, Any], *, params: Mapping[str, Any] | None = None) -> Iterable[Mapping[str, Any]]` — единый вход для получения одной сущности (допускает сложные ключи).
  - `fetch_many(*, params: Mapping[str, Any] | None = None, page_size: int | None = None) -> Iterable[Mapping[str, Any]]` — последовательное получение без ограничений по объёму.
  - `iter_pages(*, params: Mapping[str, Any] | None = None, page_size: int | None = None) -> Iterable[list[Mapping[str, Any]]]` — постраничный итератор (для провайдеров с тяжёлыми страницами).
  - `search(query: Mapping[str, Any] | str, *, page_size: int | None = None) -> Iterable[Mapping[str, Any]]` — поисковые запросы.
  - `metadata: Mapping[str, Any]` и `close() -> None`.

- `ConfigurableProvider` (Protocol mixin): `with_timeout`, `with_retries`, `with_auth`, `with_pagination(page_key, next_key, page_param, page_size)` возвращают новый конфигурированный экземпляр или обёртку.

- Исключения: ввести `DataProviderError`, `TransportError`, `PaginationError`, `AuthError` (унаследованные от `Exception`) и использовать их во всех клиентах; логировать через единый `structlog` контекст (`source`, `operation`, `path`).

**Базовые классы**
- `BaseDataProvider`: реализует `close`, хранит `options` (таймаут, ретраи, пагинация), оборачивает вызовы в `_wrap_callable/_wrap_iterator` для единообразного логирования/исключений.
- `PagedDataProvider`: добавляет `_iterate_pages` и стандартную нормализацию ответа (поддержка ключей `page_key/next_key/page_param` и plain-списков).
- `RouteDataProvider(RouteConfigurable)`: конфигурируемые маршруты как в `RouteProviderMixin`, но с унифицированным контрактом возвращаемых значений и ошибок.

**Сигнатуры и результаты**
- Все `fetch_*` возвращают **итерируемый поток словарей** (`Iterable[Mapping[str, Any]]`), даже для одиночных ответов (один элемент), чтобы унифицировать обработку пайплайнами.
- Параметры пагинации единообразны: `page_size`, `page_key="results"`, `next_key="next"`, `page_param="page"` с возможностью переопределения через конфигураторы.
- Ошибки HTTP/валидации заворачиваются в `DataProviderError` с сохранением исходной причины.

## Предлагаемые изменения в коде
**ChEMBL**
- Реализовать адаптер `ChemblDataProvider` поверх `BaseChemblClient`, соблюдая новый протокол (`fetch_many` → поток записей, `fetch_one` → один элемент-итератор, `search` принимает как `Mapping`/`str`).
- Сохранить алиасы `fetch_page/list/fetch_all/fetch_by_ids` через `DeprecatedAliasMixin`-подобный слой для обратной совместимости, но пометить как deprecated.
- Стандартировать параметры пагинации по умолчанию (поддержать старые значения через конфигуратор `with_pagination`).

**Crossref/OpenAlex/PubChem/PubMed/SemanticScholar/UniProt**
- Обновить `create_route_provider_class` так, чтобы генерируемые классы наследовали новый `RouteDataProvider` и реализовали `DataProvider`/`ConfigurableProvider`.
- Переименовать публичные методы: `fetch` → `fetch_one`, `search` → `search` (оставить алиасы через `DeprecatedAliasMixin`).
- Убедиться, что все возвращают итераторы словарей и используют единые ключи пагинации по умолчанию; специфичные провайдеры могут указывать свои значения через `with_pagination`.

**Фабрики и фасады**
- Расширить `ClientFactory`/`register_factory` до работы с новым протоколом, добавить типизацию на `DataProvider`.
- `EnricherFacade` и стратегии: принимать `DataProvider`-совместимые клиенты; добавить поддержку контекстных опций (таймаут/ретраи) через `ConfigurableProvider`.
- Ввести модуль `bioetl.clients.interfaces` с определениями протоколов/исключений; считать его референсом.

**Расширения для особых провайдеров**
- ChEMBL: сохранить возможность выбора стратегии пагинации (`pagination_strategy_name`) и сложные пути `path_template` для батчей — расширить `DataProvider` доп. методом `fetch_batch(ids, *, path_template=None)` с разумными дефолтами и optional support в других провайдерах.
- Провайдеры литературы (Crossref, PubMed и др.): поддержать специфичные query params через аргумент `query: Mapping | str` в `search` и возможный `route_name` для альтернативных эндпоинтов.

## План миграции и совместимость
1. **Введение интерфейсов**: добавить `DataProvider`, `ConfigurableProvider`, исключения и базовые классы в новом модуле без изменения существующих клиентов; обновить документацию и типизацию фабрик.
2. **Адаптеры клиентов**: создать тонкие адаптеры для ChEMBL и route-провайдеров, которые реализуют новый протокол, но внутри делегируют текущим методам; включить алиасы/DeprecationWarning для старых имён.
3. **Переход потребителей**: постепенно переводить пайплайны/CLI на новый интерфейс, начиная с мест, где уже используется фабрика (`register_domain_factories`), сохраняя старые вызовы через адаптеры.
4. **Унификация ошибок и логирования**: внедрить `DataProviderError` и унифицированный контекст логирования; обновить клиентов на использование базовых обёрток.
5. **Очистка**: после завершения перехода удалить устаревшие алиасы (`fetch_page/list/fetch_by_ids`, `fetch`/`search` в провайдерах) и deprecation-слой в `common.py`.
6. **Стабильные API**: задокументировать как стабильные `DataProvider`/`ConfigurableProvider`, базовые конфигураторы (`with_timeout/with_retries/with_pagination/with_auth`), методы `fetch_one/fetch_many/iter_pages/search`, и формат результатов (итераторы словарей).
