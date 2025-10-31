# Структура конфигураций

## обзор

Конфигурации пайплайнов описываются в YAML и валидируются строгой моделью `PipelineConfig` ([ref: repo:src/bioetl/configs/models.py@test_refactoring_32]). Параметры **MUST** собираться через механизм наследования `extends`, чтобы исключить дублирование и гарантировать единообразие таймаутов, ретраев и детерминизма.

## дерево-конфигов

| Каталог | Назначение |
| --- | --- |
| `src/bioetl/configs/base.yaml` | Базовые параметры HTTP, кеша, путей и детерминизма. |
| `src/bioetl/configs/includes/` | Повторно пригодные блоки (`chembl_source.yaml`, `determinism.yaml`, `cache.yaml`, `fallback_columns.yaml`). |
| `src/bioetl/configs/pipelines/` | Тонкие настройки конкретного пайплайна (`activity.yaml`, `document.yaml`, `target.yaml`, и т.д.). |
| `src/bioetl/configs/profiles/` | Рабочие профили окружений (`dev.yaml`, `prod.yaml`, `test.yaml`, `document_test.yaml`). |

## обязательные-правила

- Каждая конфигурация пайплайна **MUST** включать `../base.yaml` и `../includes/determinism.yaml` (см. [ref: repo:src/bioetl/configs/pipelines/activity.yaml@test_refactoring_32]).
- Блок `sources` **MUST** описывать параметры API (base_url, batch_size, лимиты) и контакты (`tool`, `email`, `mailto`); ссылки на переменные окружения оформляются как `${VAR}`.
- Политики детерминизма (`determinism.sort`, `determinism.column_order`) **MUST** соответствовать Pandera-схемам и сортировке бизнес-ключей.
- Включение/отключение обогащений следует выполнять через `sources.<name>.enabled`. Отсутствие ключа трактуется как `true`.
- Конфиги **MUST NOT** содержать неописанные поля: `ConfigDict(extra="forbid")` блокирует лишние ключи ([ref: repo:src/bioetl/configs/models.py@test_refactoring_32]).

## примеры-профилей

### dev

```yaml
extends:
  - ../base.yaml
  - ../includes/cache.yaml

http:
  global:
    timeout_sec: 30.0
    retries:
      total: 2
      backoff_multiplier: 1.5
      backoff_max: 30.0
    rate_limit:
      max_calls: 10
      period: 10.0

cache:
  ttl: 3600
```

- Пониженные таймауты и ретраи ускоряют локальную разработку.
- Кеш держится один час; release scope отключён для быстрого прототипирования.

### document_test

```yaml
extends:
  - ../pipelines/document.yaml

sources:
  pubmed:
    enabled: false
    tool: "test-tool"
    email: "test@example.com"
  crossref:
    enabled: false

postprocess:
  qc:
    enabled: false

qc:
  enabled: false
```

- Профиль отключает все внешние API, обеспечивая офлайн-тесты (`tests/sources/document/test_pipeline_e2e.py`).
- Значения `tool/email` заполнены фиктивными данными, чтобы `PipelineConfig` прошёл валидацию.

## env-override

- Любой ключ может быть переопределён переменной окружения `BIOETL__SECTION__KEY` ([ref: repo:src/bioetl/config/loader.py@test_refactoring_32]).
- Контактные данные (`tool/email/mailto`) **MUST** разрешаться через `env:` или `${VAR}` благодаря `field_validator` в `Source` ([ref: repo:src/bioetl/configs/models.py@test_refactoring_32]). Отсутствующий секрет вызывает исключение при загрузке.

## валидация

- CLI по умолчанию проверяет конфиг командой `python -m bioetl.cli.main <pipeline> --config ... --dry-run --validate-columns`.
- Дополнительно доступен скрипт `python src/scripts/validate_columns.py --entity all --schema-version latest` для полного сверения с Pandera.

## best-practices

- Дублирующиеся блоки выносите в `includes/`; `fallback_columns.yaml` уже переиспользуется несколькими пайплайнами.
- При добавлении новых источников фиксируйте лимиты и ретраи сразу в конфиге, чтобы документ `03-data-sources-and-spec.md` оставался источником истины.
- Профили окружений **SHOULD** отличаться только параметрами времени ожидания, кеша и троттлинга.


