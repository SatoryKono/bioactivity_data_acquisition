#  clients

## Таблица A — файлы и реализованные объекты (исправлённое)

(пути относительно `src/bioetl/clients/`)

| Файл                                             | Экспортируемые объекты (ключевые)                                                                                                                                                            | Краткое назначение                                                                                                                  |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `enricher_base.py`                               | `EnricherClientOptions`, `OptionsAwareApiClient`, `BaseEnricherClient`, `UnifiedProviderAdapter`, `RouteConfig`, `RouteEnricherMixin`, `DeprecatedAliasMixin`, `create_route_provider_class` | Ядро enricher-провайдеров: low-level HTTP-логика, маршрутизация (ROUTES), адаптер route→DataProvider, обработка устаревших алиасов. |
| `chembl/base.py`                                 | `BaseChemblClient`, `ChemblEntityClient`, `PaginationStrategyResolverMixin`                                                                                                                  | ChEMBL-ветка: клиент для сущностей ChEMBL, поддержка plug-in пагинации (resolver).                                                  |
| `base/contracts.py`                              | `DataProviderABC` / `DataProviderProtocol`, `PaginationParams`, `Page`, `RequestContext`, `RecordStream`, `PageStream`, `RouteConfig`                                                        | **Source-of-truth** для контрактов и типов.                                                                                         |
| `base/normalizers.py` (реком.)                   | `NormalizerProtocol`, `ChainNormalizer`                                                                                                                                                      | Протокол и утилиты для нормализаторов записей. Рекомендуется `NormalizerProtocol` (без префикса `I`).                               |
| `base/http.py` (реком.)                          | `BaseHttpClientABC`, `TransportError`                                                                                                                                                        | Абстракция транспорта; обязан бросать `TransportError`.                                                                             |
| `base/pagination.py` (реком.)                    | `PaginationStrategyABC`                                                                                                                                                                      | Интерфейс стратегий пагинации.                                                                                                      |
| `providers/base_provider.py` (реком.)            | `BaseDataProviderABC`, `PagedDataProviderABC`                                                                                                                                                | Общая логика провайдеров: wrapping, нормализация страниц, итераторы.                                                                |
| `providers/<provider>/impl/*.py` (реком.)        | Concrete impl classes (суффикс `Impl`, e.g. `ChemblDataClientHTTPImpl`)                                                                                                                      | Конкретные реализации, в каталоге `impl/`.                                                                                          |
| `providers/<provider>/normalization.py` (реком.) | `ProviderNormalizer`                                                                                                                                                                         | Provider-specific нормализаторы.                                                                                                    |
| `providers/openalex.py`                          | `OpenAlexClient` (create_route_provider_class output)                                                                                                                                        | Thin module — декларация ROUTES; реализация в enricher/core.                                                                        |
| `providers/pubchem.py`                           | `PubChemClient`, deprecated aliases (например `fetch_by_cid`)                                                                                                                                | То же + deprecated aliases.                                                                                                         |
| `providers/pubmed.py`                            | `PubmedClient`, provider aliases                                                                                                                                                             | То же + provider aliases.                                                                                                           |
| `factories.py` в каждом домене (реком.)          | `default_<domain>_<entity>` (и, при необходимости, `ClientFactory` / `ClientFactoryRegistry`)                                                                                                | Default фабрики: собирают transport + provider + adapter + normalizer → готовый DataProvider.                                       |
| `base/errors.py` (реком.)                        | `TransportError`, `DataProviderError`, `PaginationError`, `ConfigurationError`                                                                                                               | Единая иерархия ошибок для clients.                                                                                                 |
| `tests/clients/*` (реком.)                       | unit/integration tests                                                                                                                                                                       | Тесты контрактов, пагинации, адаптеров, normalizers.                                                                                |

---

## Таблица B — ключевые объекты / интерфейсы (исправлённое)

> Примечание: **все** абстрактные классы должны наследовать `abc.ABC` или реализовывать `Protocol` и иметь структурированный докстринг (краткое описание, публичный интерфейс, путь к файлу, указатель на default/impl).

### 1) `DataProviderABC` / `DataProviderProtocol`

**Файл:** `base/contracts.py`
**Публичные методы (сокращённо):**

```py
def fetch_one(self, identifier: str|Mapping[str,Any], *, params: Mapping|None=None, context: RequestContext|None=None) -> Iterator[Mapping[str,Any]]
def iter_pages(self, *, params: Mapping|None=None, page_size: int|None=None, context: RequestContext|None=None) -> Iterator[Page]
def fetch_many(self, *, params: Mapping|None=None, page_size: int|None=None, context: RequestContext|None=None) -> Iterator[Mapping[str,Any]]
def search(self, query: Mapping|str, *, page_size:int|None=None, context: RequestContext|None=None) -> Iterator[Mapping[str,Any]]
def metadata(self) -> Mapping[str,Any]
def close(self) -> None
```

**Назначение:** единый ленивый API; документирован и зарегистрирован в `abc_registry.yaml` при создании.

---

### 2) `ConfigurableProviderABC`

**Файл:** `base/contracts.py`
**Методы:**

```py
def with_timeout(self, seconds: int) -> Self
def with_retries(self, retries: int) -> Self
def with_auth(self, auth: Any) -> Self
def with_pagination(self, *, page_key: str|None, next_key: str|None, page_param: str|None, page_size:int|None) -> Self
```

**Назначение:** fluent API для конфигурации клиента; явное поведение (возврат self или нового инстанса) должно быть задокументировано.

---

### 3) `BaseHttpClientABC`

**Файл:** `base/http.py`
**Методы:**

```py
def request(self, method: str, url: str, *, params=None, json=None, headers=None, context: RequestContext|None=None) -> Any
def get_json(self, url: str, *, params=None, context: RequestContext|None=None) -> Any
def paginate_json(self, url: str, *, params=None, page_key: str|None=None, next_key: str|None=None, page_param: str|None=None, context: RequestContext|None=None) -> Iterator[Any]
def close(self) -> None
@property def metadata(self) -> Mapping[str,Any] | None
```

**Поведение:** обязан бросать `TransportError`; реализовывать retry/timeout/ratelimit semantics.

---

### 4) `BaseDataProviderABC` / `PagedDataProviderABC`

**Файл:** `providers/base_provider.py`
**Ключевые методы:** `_wrap_callable`, `_wrap_iterator`, абстрактные `fetch_one`, `iter_pages`, `_iterate_pages_impl`, `_normalize_page_payload`, `fetch_many`.
**Назначение:** общая логика провайдеров и paged-провайдеров; используют типы из `base/contracts.py`.

---

### 5) `RouteProviderABC` / `RouteEnricherMixin` / `UnifiedProviderAdapter`

**Файл:** `enricher_base.py`
**Требования:**

* `ROUTES: ClassVar[Iterable[RouteConfig]]` обязателен.
* `UnifiedProviderAdapter` **реализует** `DataProviderABC` и ожидает соглашение `iter_pages(query={'value': ...})`.
* `RouteConfig` должен быть типизирован и документирован (рекомендуется вынести определение в `base/contracts.py`).

---

### 6) `BaseEnricherClient` / `EnricherClientABC`

**Файл:** `enricher_base.py`
**Методы:** `_normalize_payload`, `_iterate_pages`, `fetch_one`, `fetch_batch`, `metadata`, `close` — базовая HTTP-логика, нормализация страниц.

---

### 7) `PaginationStrategyABC`

**Файл:** `base/pagination.py`
**Методы:**

```py
def iter_pages(self, initial_response_or_args: Any, transport: BaseHttpClientABC, *, endpoint: str, params: Mapping|None=None, page_key: str|None=None, next_key: str|None=None, page_param: str|None=None, normalize: Callable|None=None) -> Iterator[Any]
def reset(self) -> None
```

**Назначение:** pluggable стратегии пагинации.

---

### 8) `NormalizerProtocol`

**Файл:** `base/normalizers.py`
**Интерфейс:**

```py
def normalize(self, record: Mapping[str,Any]) -> Mapping[str,Any]
def normalize_batch(self, records: Iterable[Mapping]) -> Iterator[Mapping]
```

**Замечание:** избегать префикса `I` в именах протоколов.

---

### 9) `DeprecatedAliasMixin`

**Файл:** `enricher_base.py`
**Поведение:** `__getattr__` для проксирования deprecated names; документировать поведение.

---

### 10) `ClientFactory` / `default_<domain>_<entity>`

**Файл:** `src/bioetl/clients/<domain>/factories.py`
**Методы:**

```py
def default_<domain>_<entity>(...) -> DataProviderABC
def register_factory(self, name: str, factory: Callable[...,DataProviderABC]) -> None
def get(self, name: str) -> Callable[...,DataProviderABC] | None
```

**Назначение:** canonical entrypoint для создания готового клиента; **обязательное** наличие `default_<domain>_<entity>` для каждого entity; регистрация в `abc_impls.yaml`.

---

## Таблица C — обязательные артефакты / требования (исправлённое)

| Артефакт                  |                                                          Путь | Почему нужен / примечание                                                                                    |
| ------------------------- | ------------------------------------------------------------: | ------------------------------------------------------------------------------------------------------------ |
| Errors                    |                                              `base/errors.py` | `TransportError`, `DataProviderError`, `PaginationError`, `ConfigurationError` — единая иерархия ошибок.     |
| Types / Contracts         |                                           `base/contracts.py` | `Page`, `PaginationParams`, `RequestContext`, `RecordStream`, `PageStream`, `RouteConfig` — source-of-truth. |
| Transport implementations |                                 `core/http/` + `base/http.py` | `BaseHttpClientABC`, `OptionsAwareApiClient`, `paginate_json` с retries/backoff/timeout.                     |
| Base provider ABCs        |                                  `providers/base_provider.py` | `BaseDataProviderABC`, `PagedDataProviderABC` — общая логика провайдеров.                                    |
| Per-provider normalizers  |                       `providers/<provider>/normalization.py` | Provider-specific normalizers (PubChem, UniProt, ChEMBL и т.д.).                                             |
| Impl-директория           |                                  `providers/<provider>/impl/` | Concrete impls с суффиксом `Impl` (например `ChemblDataClientHTTPImpl`).                                     |
| Factories (Default)       |                    `src/bioetl/clients/<domain>/factories.py` | `default_<domain>_<entity>` — canonical factory для каждого entity.                                          |
| Registries                | `src/bioetl/clients/base/abc_registry.yaml`, `abc_impls.yaml` | Машинные реестры ABC/Impls; обновлять при добавлении/изменении.                                              |
| Docs & examples           |                               `docs/` (kebab-case, NN-prefix) | Обновлять docs при изменениях контрактов/Defaults/Impls; H1 = filename Title Case.                           |
| Tests                     |                                             `tests/clients/*` | Unit/integration tests; покрытие контрактов, пагинаций, адаптеров, нормализаторов.                           |
| Linter / CI checks        |                            `.pre-commit-config.yaml`, CI jobs | Проверка нейминга, докстрингов ABC, наличие `default_*`, синхронизация реестров.                             |

---

## Краткие рекомендации / план внедрения (исправлённое)

1. Консолидировать все общие контракты и типы в `src/bioetl/clients/base/contracts.py`.
2. Гарантировать, что все ABC/Protocol наследуют `abc.ABC` или `Protocol` и содержат структурированный докстринг (описание, интерфейс, локализация, pointer на default/impl).
3. Ввести `BaseHttpClientABC` в `base/http.py` и реализовать транспортную инфраструктуру (`core/http/`).
4. Реализовать `BaseDataProviderABC` / `PagedDataProviderABC` в `providers/base_provider.py` и адаптировать существующие клиенты под эти контракты.
5. Для каждого домена создать `src/bioetl/clients/<domain>/factories.py` с функциями `default_<domain>_<entity>`.
6. Переместить конкретные реализации в `providers/<provider>/impl/` с суффиксом `Impl`.
7. Добавить per-provider normalizers в `providers/<provider>/normalization.py`.
8. Обновить `abc_registry.yaml`, `abc_impls.yaml`, `docs/ABC_INDEX.md` и соответствующие pipeline docs в одном PR при изменениях.
9. Написать/обновить тесты и настроить CI проверки (название классов/файлов, докстринги ABC, наличие default-фабрик, синхронизация реестров).

---

## References





