# Переход REST-клиентов на YAML-конфигурации и фабрику

## Сводка
- Все параметры REST-вызовов (base_url, пути, методы, заголовки, схема ответа, поля) выносятся в YAML per-source. Клиенты становятся конфигурационными обёртками, читающими SourceConfig.
- Общие pydantic-модели `SourceConfig`/`ResourceConfig`/`PagingConfig` и загрузчики YAML обеспечивают единообразие и раннюю валидацию.
- Фабрика создаёт клиентов по имени источника, подключая общий HTTP-бэкенд; в коде клиентов нет захардкоженных URL или параметров.

## Структура YAML-конфигов для REST
```yaml
source: chembl
protocol: http
base_url: "https://www.ebi.ac.uk/chembl/api/data"
default_timeout: 30.0
auth:
  type: none  # none | api_key | bearer | basic | custom
rate_limit:
  requests_per_minute: 60
resources:
  activity:
    path: "/activity"
    method: GET
    query:
      fixed:
        format: json
      allowed_params:
        - target_chembl_id
        - assay_chembl_id
    paging:
      type: page           # page | offset | cursor | link | none
      page_param: page
      page_size_param: page_size
      default_page_size: 1000
      max_page_size: 1000
    response:
      format: json         # json | xml
      record_path: activities
      fields:
        - name: activity_id
          path: activity_id
          type: int        # str | int | float | bool | any
      extra_metadata:
        - name: page
          path: page_info.page
          type: int
```
Ключевые поля:
- `source`, `protocol`, `base_url`, `default_timeout`, `auth`, `rate_limit` — общие настройки источника.
- `resources` — словарь маршрутов: `path`, `method`, `headers`, `auth` (override), `query.fixed/allowed/rename`, `paging` с разными стратегиями, `response` с `record_path` и описанием полей.
- Все маршруты, параметры и схема ответа описаны в YAML; код клиентов ничего не знает о конкретных эндпоинтах.

## Python-модели конфигурации
- Пакет `bioetl.clients.config` содержит pydantic-модели (`SourceConfig`, `ResourceConfig`, `PagingConfig`, `ResponseConfig`, `FieldConfig`, `AuthConfig`, `RateLimitConfig`, `QueryConfig`) и загрузчики YAML (`load_source_config`, `load_all_sources`).
- Жёсткая валидация: `extra="forbid"`, обязательные поля для выбранной стратегии пагинации (например, `page_param` для `type=page`).
- Базовые значения по умолчанию: `protocol="http"`, `auth.type="none"`, `response.format="json"`, пустые заголовки/фиксированные параметры.

Пример использования:
```python
from bioetl.clients.config import load_source_config

cfg = load_source_config("chembl")
print(cfg.resources["activity"].paging.page_size_param)  # -> "page_size"
```

## Фабрика REST-клиентов
- Модуль `bioetl.clients.factory` предоставляет `create_client(source, *, config=None, http_backend=None, context=None) -> ExternalDataClient`.
- Фабрика загружает `SourceConfig` из YAML (если не передан), получает/создаёт `HttpBackend` и строит тонкий `ConfiguredRestClient`, который делегирует вызовы `fetch_one`/`fetch_many`/`iter_pages` HTTP-бэкенду через `ResourceConfig`.
- Через `ClientFactoryContext.registry` можно переопределить конкретный класс клиента для источника, не меняя сигнатуры контракта.

## Что нельзя держать в клиентах
- Доменные нормализации, маппинг на модели, фильтрации и агрегации — выносятся в пайплайн/сервисы.
- Константы эндпоинтов, заголовков, query/пагинации — только в YAML.
- Логика ретраев/логирования/трассировки — реализуется на уровне общего `HttpBackend`.

## План миграции REST-параметров в YAML
1. Собрать текущие значения base_url/эндпоинтов/параметров/пагинации из кода и конфигов (источник истины — существующие константы и pipeline defaults).
2. Создать YAML `configs/clients/<source>.yaml`, перенести туда base_url, ресурсы, схему ответа, правила пагинации и query-параметров.
3. Описать модели/loader (готово) и переписать клиентов на `SourceConfig`+`HttpBackend` (через `ConfiguredRestClient`).
4. Удалить захардкоженные константы и старые формат конфигов в коде/провайдерах.

Таблица соответствия:

| Источник | Текущий путь параметров (константы/настройки) | Новый YAML-файл | Новый модуль конфигурации |
| --- | --- | --- | --- |
| ChEMBL | `configs/defaults/chembl.yaml`, `configs/chembl.yaml`, параметры транспорта в `src/bioetl/clients/chembl/factories.py` и маршруты в `src/bioetl/clients/providers/chembl.py` | `configs/clients/chembl.yaml` | `bioetl.clients.config.models.SourceConfig` |
| PubChem | `configs/pubchem.yaml`, маршруты `src/bioetl/clients/providers/pubchem.py`, base_url в `configs/README.md` | `configs/clients/pubchem.yaml` | `bioetl.clients.config.models.SourceConfig` |
| PubMed | Маршруты `src/bioetl/clients/providers/pubmed.py`, base_url и лимиты в `configs/defaults/sources.yaml` | `configs/clients/pubmed.yaml` | `bioetl.clients.config.models.SourceConfig` |
| OpenAlex | Маршруты `src/bioetl/clients/providers/openalex.py`, base_url в `configs/defaults/sources.yaml` | `configs/clients/openalex.yaml` | `bioetl.clients.config.models.SourceConfig` |
| Crossref | Параметры в `configs/defaults/sources.yaml`, логика пагинации в `src/bioetl/clients/crossref/client.py` | `configs/clients/crossref.yaml` | `bioetl.clients.config.models.SourceConfig` |
| Semantic Scholar | Параметры и маршруты в `src/bioetl/clients/semantic_scholar/client.py` и `configs/semantic_scholar.yaml` | `configs/clients/semantic_scholar.yaml` | `bioetl.clients.config.models.SourceConfig` |
| UniProt | base_url в `configs/defaults/sources.yaml`, маршруты в `src/bioetl/clients/providers/uniprot.py` | `configs/clients/uniprot.yaml` | `bioetl.clients.config.models.SourceConfig` |

## Валидация и health-checks конфигураций
- Схемы YAML валидируются pydantic-моделями (`extra="forbid"`, обязательные поля для выбранной пагинации, запрет пустого `resources`).
- Автоматические проверки:
  - загрузка всех YAML через `load_all_sources` в CI для раннего обнаружения ошибок формата;
  - проверка `method` ∈ {GET, POST, PUT, DELETE, PATCH}, `protocol` = http;
  - проверка, что `record_path` и `fields.path` присутствуют в примере ответа (health-check скрипт выполняет тестовый запрос через общий `HttpBackend` в sandbox-окружении);
  - smoke-test: `create_client(<source>)` с тестовым backend выполняет один запрос и убеждается, что пагинация даёт ожидаемую схему `Page`/`Record` без нормализации.
