# Specification: Typed Configurations and Profiles

> **Note**: Implementation status: **implemented**. The configuration system is fully implemented with a modular structure.

This document provides a comprehensive specification for the `bioetl` configuration system, based on the implementation in `[ref: repo:src/bioetl/config/loader.py]`.

## 1. Обзор и цели

Типобезопасный объект `PipelineConfig` формирует единый контракт между CLI и пайплайнами: Typer-команда при запуске автоматически подмешивает профили `base.yaml` и `determinism.yaml`, затем валидирует объединённые данные строго по модели `PipelineConfig`.[ref: repo:README.md†L18-L29][ref: base-config] Благодаря Pydantic-вёрстке (`extra="forbid"`) любой неизвестный ключ приводит к немедленной ошибке, а кросс-полевые инварианты (например, согласованность сортировки) проверяются валидаторами.

### 1.1 Структура модулей

Начиная с версии 1.1 конфигурационные модели сгруппированы в два ключевых файла:

- `src/bioetl/config/models/models.py` — базовые секции конфигурации (метаданные пайплайна, параметры рантайма, ввод/вывод, пути, логирование, CLI, телеметрия и т.д.).
- `src/bioetl/config/models/policies.py` — политики поведения (HTTP-профили, ретраи, rate limit, гарантии детерминизма, fallback-механизмы).

Иерархия `src/bioetl/config/models/__init__.py` экспонирует упрощённую публичную поверхность и одновременно создаёт обёртки для старых путей (`base`, `http`, `determinism` и пр.). Эти обёртки помечены как *deprecated* и будут удалены в **bioetl 2.0**; при импорте они выбрасывают `DeprecationWarning`, но продолжают работать на переходный период.

### 1.2 Pipeline-специфичные конфигурации

Pipeline-специфичные конфигурации находятся в подпапках:

- `src/bioetl/config/assay/` - `AssaySourceConfig`, `AssaySourceParameters`
- `src/bioetl/config/activity/` - `ActivitySourceConfig`, `ActivitySourceParameters`
- `src/bioetl/config/target/` - `TargetSourceConfig`, `TargetSourceParameters`
- `src/bioetl/config/document/` - `DocumentSourceConfig`, `DocumentSourceParameters`
- `src/bioetl/config/testitem/` - `TestItemSourceConfig`, `TestItemSourceParameters`

## 2. Структура и типы `PipelineConfig`

### 2.1 Корневые ключи

| Section | Key | Type | Required | Default | Description |
| --- | --- | --- | --- | --- | --- |
| `PipelineConfig` | `version` | `Literal[1]` | Yes | — | Версия схемы конфигурации.[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `extends[]` | `Sequence[str]` | No | `[]` | Профили, которые мерджатся перед основным YAML.[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `pipeline` | `PipelineMetadata` | Yes | — | Метаданные пайплайна.[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `runtime` | `RuntimeConfig` | No | см. таблицу ниже | Параметры исполнения (параллелизм, чанки).[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `io` | `IOConfig` | No | см. таблицу ниже | Политика ввода/вывода (форматы, partitioning).[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `http` | `HTTPConfig` | Yes | — | HTTP-профили и базовые настройки клиентов.[ref: repo:src/bioetl/config/models/policies.py] |
| `PipelineConfig` | `cache` | `CacheConfig` | No | см. таблицу ниже | Параметры HTTP-кэша.[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `paths` | `PathsConfig` | No | см. таблицу ниже | Каталоги ввода/вывода.[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `determinism` | `DeterminismConfig` | No | см. таблицу ниже | Политика детерминизма выгрузок.[ref: repo:src/bioetl/config/models/policies.py] |
| `PipelineConfig` | `materialization` | `MaterializationConfig` | No | см. таблицу ниже | Настройки записи артефактов.[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `fallbacks` | `FallbacksConfig` | No | см. таблицу ниже | Поведение fallback-механизмов.[ref: repo:src/bioetl/config/models/policies.py] |
| `PipelineConfig` | `validation` | `ValidationConfig` | No | см. таблицу ниже | Ссылки на Pandera-схемы и строгий режим.[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `sources{}` | `Dict[str, SourceConfig]` | No | `{}` | Переопределения для отдельных источников.[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `cli` | `CLIConfig` | No | см. таблицу ниже | Захваченные значения CLI-флагов.[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `logging` | `LoggingConfig` | No | см. таблицу ниже | Настройки UnifiedLogger (уровень, формат).[ref: repo:src/bioetl/config/models/models.py] |
| `PipelineConfig` | `telemetry` | `TelemetryConfig` | No | см. таблицу ниже | Настройки экспорта OpenTelemetry.[ref: repo:src/bioetl/config/models/models.py] |

### 2.2 `pipeline`

| Key | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | `str` | Yes | — | Уникальное имя пайплайна.[ref: repo:src/bioetl/config/models/models.py] |
| `version` | `str` | Yes | — | Семантическая версия реализации.[ref: repo:src/bioetl/config/models/models.py] |
| `owner` | `str` | No | `null` | Ответственный инженер/команда.[ref: repo:src/bioetl/config/models/models.py] |
| `description` | `str` | No | `null` | Краткое описание.[ref: repo:src/bioetl/config/models/models.py] |

### 2.3 `runtime`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `parallelism` | `PositiveInt` | `4` | Количество параллельных воркеров для пайплайна.[ref: repo:src/bioetl/config/models/models.py] |
| `chunk_rows` | `PositiveInt` | `100000` | Размер чанка записей при батчевой обработке.[ref: repo:src/bioetl/config/models/models.py] |
| `dry_run` | `bool` | `false` | Включает режим без записи артефактов.[ref: repo:src/bioetl/config/models/models.py] |
| `seed` | `int` | `42` | Детерминированный seed для случайностей.[ref: repo:src/bioetl/config/models/models.py] |

### 2.4 `io`

| Section | Key | Type | Default | Description |
| --- | --- | --- | --- | --- |
| `input` | `format` | `str` | `csv` | Формат локального ввода (csv/parquet/json).[ref: repo:src/bioetl/config/models/models.py] |
|  | `encoding` | `str` | `utf-8` | Кодировка входных файлов.[ref: repo:src/bioetl/config/models/models.py] |
|  | `header` | `bool` | `true` | Ожидается ли строка заголовков.[ref: repo:src/bioetl/config/models/models.py] |
|  | `path` | `str \| None` | `null` | Путь до конкретного входного файла (опционально).[ref: repo:src/bioetl/config/models/models.py] |
| `output` | `format` | `str` | `parquet` | Формат выгрузок.[ref: repo:src/bioetl/config/models/models.py] |
|  | `partition_by[]` | `Sequence[str]` | `[]` | Колонки для partitioning (стабильный порядок).[ref: repo:src/bioetl/config/models/models.py] |
|  | `overwrite` | `bool` | `true` | Разрешено ли перезаписывать существующие артефакты.[ref: repo:src/bioetl/config/models/models.py] |
|  | `path` | `str \| None` | `null` | Принудительный путь вывода (для отладки).[ref: repo:src/bioetl/config/models/models.py] |

### 2.5 `http`

| Key | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `default.timeout_sec` | `PositiveFloat` | No | `60.0` | Общий таймаут запроса.[ref: repo:src/bioetl/config/models/policies.py] |
| `default.connect_timeout_sec` | `PositiveFloat` | No | `15.0` | Таймаут соединения.[ref: repo:src/bioetl/config/models/policies.py] |
| `default.read_timeout_sec` | `PositiveFloat` | No | `60.0` | Таймаут чтения сокета.[ref: repo:src/bioetl/config/models/policies.py] |
| `default.retries.*` | `RetryConfig` | No | см. подтаблицу | Политика повторов.[ref: repo:src/bioetl/config/models/policies.py] |
| `default.rate_limit.*` | `RateLimitConfig` | No | см. подтаблицу | Ограничение запросов.[ref: repo:src/bioetl/config/models/policies.py] |
| `default.rate_limit_jitter` | `bool` | No | `true` | Добавляет джиттер к лимитам.[ref: repo:src/bioetl/config/models/policies.py] |
| `default.headers{}` | `Mapping[str, str]` | No | см. значение | Базовые заголовки HTTP.[ref: repo:src/bioetl/config/models/policies.py] |
| `profiles.<name>` | `HTTPClientConfig` | No | `{}` | Именованные профили для источников.[ref: repo:src/bioetl/config/models/policies.py] |

**`RetryConfig`**

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `total` | `PositiveInt` | `5` | Количество повторов.[ref: repo:src/bioetl/config/models/policies.py] |
| `backoff_multiplier` | `PositiveFloat` | `2.0` | Множитель экспоненциального backoff.[ref: repo:src/bioetl/config/models/policies.py] |
| `backoff_max` | `PositiveFloat` | `60.0` | Максимальная задержка между попытками.[ref: repo:src/bioetl/config/models/policies.py] |
| `statuses[]` | `Tuple[int]` | `(408,429,500,502,503,504)` | Коды, запускающие повтор.[ref: repo:src/bioetl/config/models/policies.py] |

**`RateLimitConfig`**

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `max_calls` | `PositiveInt` | `10` | Запросов в окне.[ref: repo:src/bioetl/config/models/policies.py] |
| `period` | `PositiveFloat` | `1.0` | Длина окна в секундах.[ref: repo:src/bioetl/config/models/policies.py] |

### 2.6 Инфраструктурные блоки

| Section | Key | Default | Description |
| --- | --- | --- | --- |
| `cache` | `enabled` | `true` | Вкл./выкл. дискового кэша.[ref: repo:src/bioetl/config/models/models.py] |
|  | `directory` | `"http_cache"` | Каталог кэша.[ref: repo:src/bioetl/config/models/models.py] |
|  | `ttl` | `86400` | TTL записи (сек).[ref: repo:src/bioetl/config/models/models.py] |
| `paths` | `input_root` | `"data/input"` | Базовый каталог входных данных.[ref: repo:src/bioetl/config/models/models.py] |
|  | `output_root` | `"data/output"` | Базовый каталог выгрузок.[ref: repo:src/bioetl/config/models/models.py] |
|  | `cache_root` | `".cache"` | Каталог временных файлов.[ref: repo:src/bioetl/config/models/models.py] |
| `materialization` | `root` | `"data/output"` | Корень артефактов.[ref: repo:src/bioetl/config/models/models.py†L119-L123] |
|  | `default_format` | `"parquet"` | Формат по умолчанию.[ref: repo:src/bioetl/config/models/models.py†L119-L123] |
|  | `pipeline_subdir` | `null` | Доп. подкаталог.[ref: repo:src/bioetl/config/models/models.py†L124-L127] |
|  | `filename_template` | `null` | Jinja-шаблон имени файла.[ref: repo:src/bioetl/config/models/models.py†L128-L131] |
| `fallbacks` | `enabled` | `true` | Включает fallback-стратегии.[ref: repo:src/bioetl/config/models/models.py†L139-L146] |
|  | `max_depth` | `null` | Ограничение глубины fallback.[ref: repo:src/bioetl/config/models/models.py†L139-L146] |
| `validation` | `schema_in` | `null` | Путь к входной Pandera-схеме.[ref: repo:src/bioetl/config/models/models.py†L287-L296] |
|  | `schema_out` | `null` | Путь к выходной схеме.[ref: repo:src/bioetl/config/models/models.py†L291-L296] |
|  | `strict` | `true` | Требует строгого порядка колонок.[ref: repo:src/bioetl/config/models/models.py†L295-L296] |
|  | `coerce` | `true` | Приводит типы в Pandera.[ref: repo:src/bioetl/config/models/models.py†L295-L296] |
| `cli` | `profiles[]` | `[]` | Профили, переданные через `--profile`.[ref: repo:src/bioetl/config/models/models.py†L304-L316] |
|  | `dry_run` | `false` | Флаг `--dry-run`.[ref: repo:src/bioetl/config/models/models.py†L308-L316] |
|  | `limit` | `null` | Лимит записей (`--limit`).[ref: repo:src/bioetl/config/models/models.py†L309-L312] |
|  | `set_overrides{}` | `{}` | Пары `--set key=value`.[ref: repo:src/bioetl/config/models/models.py†L313-L316] |
| `sources.<id>` | `enabled` | `true` | Отключение источника.[ref: repo:src/bioetl/config/models/models.py†L324-L341] |
|  | `description` | `null` | Описание источника.[ref: repo:src/bioetl/config/models/models.py†L325-L341] |
|  | `http_profile` | `null` | Ссылка на HTTP-профиль.[ref: repo:src/bioetl/config/models/models.py†L326-L333] |
|  | `http` | `null` | Inline-переопределения HTTP (`HTTPClientConfig`).[ref: repo:src/bioetl/config/models/models.py†L330-L333] |
|  | `batch_size` | `null` | Размер батча для пагинации.[ref: repo:src/bioetl/config/models/models.py†L334-L337] |
|  | `parameters{}` | `{}` | Произвольные параметры источника.[ref: repo:src/bioetl/config/models/models.py†L338-L341] |

### 2.7 `determinism`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `true` | Включает гарантии детерминизма.[ref: repo:src/bioetl/config/models/models.py†L246-L279] |
| `hash_policy_version` | `str` | `"1.0.0"` | Версионирование стратегии хеширования.[ref: repo:src/bioetl/config/models/models.py†L247-L250] |
| `float_precision` | `PositiveInt` | `6` | Количество знаков после запятой при нормализации.[ref: repo:src/bioetl/config/models/models.py†L251-L254] |
| `datetime_format` | `str` | `"iso8601"` | Формат сериализации дат.[ref: repo:src/bioetl/config/models/models.py†L255-L258] |
| `column_validation_ignore_suffixes[]` | `Sequence[str]` | `("_scd","_temp","_meta","_tmp")` | Суффиксы, игнорируемые при проверке колонок.[ref: repo:src/bioetl/config/models/models.py†L259-L262] |
| `sort.by[]` | `List[str]` | `[]` | Столбцы сортировки.[ref: repo:src/bioetl/config/models/models.py†L172-L183] |
| `sort.ascending[]` | `List[bool]` | `[]` | Направление сортировки; должно совпадать по длине с `sort.by`.[ref: repo:src/bioetl/config/models/models.py†L172-L279] |
| `sort.na_position` | `str` | `"last"` | Положение `NA` при сортировке.[ref: repo:src/bioetl/config/models/models.py†L172-L183] |
| `column_order[]` | `Sequence[str]` | `[]` | Жёстко фиксированный порядок колонок (требует `validation.schema_out`).[ref: repo:src/bioetl/config/models/models.py†L263-L267][ref: column-order-validation] |
| `serialization.csv.separator` | `str` | `","` | Разделитель CSV.[ref: repo:src/bioetl/config/models/models.py†L154-L169] |
| `serialization.csv.quoting` | `str` | `"ALL"` | Стратегия кавычек.[ref: repo:src/bioetl/config/models/models.py†L154-L156] |
| `serialization.csv.na_rep` | `str` | `""` | Представление `NA` в CSV.[ref: repo:src/bioetl/config/models/models.py†L154-L156] |
| `serialization.booleans[]` | `Tuple[str,str]` | `("True","False")` | Строковые значения для bool.[ref: repo:src/bioetl/config/models/models.py†L164-L168] |
| `serialization.nan_rep` | `str` | `"NaN"` | Представление `NaN`.[ref: repo:src/bioetl/config/models/models.py†L164-L169] |
| `hashing.algorithm` | `str` | `"sha256"` | Алгоритм хеширования.[ref: repo:src/bioetl/config/models/models.py†L185-L201] |
| `hashing.row_fields[]` | `Sequence[str]` | `[]` | Поля для `hash_row`.[ref: repo:src/bioetl/config/models/models.py†L190-L197] |
| `hashing.business_key_fields[]` | `Sequence[str]` | `[]` | Поля бизнес-ключа.[ref: repo:src/bioetl/config/models/models.py†L195-L198] |
| `hashing.exclude_fields[]` | `Sequence[str]` | `("generated_at","run_id")` | Поля, исключаемые из хешей.[ref: repo:src/bioetl/config/models/models.py†L199-L201] |
| `environment.timezone` | `str` | `"UTC"` | Часовой пояс исполнения.[ref: repo:src/bioetl/config/models/models.py†L205-L212] |
| `environment.locale` | `str` | `"C"` | Локаль для форматирования.[ref: repo:src/bioetl/config/models/models.py†L205-L212] |
| `write.strategy` | `str` | `"atomic"` | Стратегия записи артефактов.[ref: repo:src/bioetl/config/models/models.py†L214-L220] |
| `meta.location` | `str` | `"sibling"` | Расположение `meta.yaml`.[ref: repo:src/bioetl/config/models/models.py†L222-L238] |
| `meta.include_fields[]` | `Sequence[str]` | `[]` | Поля, обязанные попасть в `meta`.[ref: repo:src/bioetl/config/models/models.py†L231-L234] |
| `meta.exclude_fields[]` | `Sequence[str]` | `[]` | Поля, исключаемые перед хешированием.[ref: repo:src/bioetl/config/models/models.py†L235-L238] |

Дополнительные инварианты:

- `determinism.sort.ascending` должно быть пустым или совпадать по длине с `determinism.sort.by`; нарушение приводит к ValueError.[ref: repo:src/bioetl/config/models/models.py†L274-L279]
- Если задан `determinism.column_order`, необходимо указать `validation.schema_out`, иначе валидация отклонит конфиг.[ref: repo:src/bioetl/config/models/models.py†L381-L388]

### 2.8 `logging`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `level` | `str` | `"INFO"` | Уровень UnifiedLogger.[ref: repo:src/bioetl/config/models/models.py] |
| `format` | `str` | `"json"` | Формат вывода (`json`/`console`).[ref: repo:src/bioetl/config/models/models.py] |
| `with_timestamps` | `bool` | `true` | Включает UTC метки времени.[ref: repo:src/bioetl/config/models/models.py] |
| `context_fields[]` | `Sequence[str]` | `(pipeline, run_id)` | Обязательные поля контекста в логе.[ref: repo:src/bioetl/config/models/models.py] |

### 2.9 `telemetry`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `false` | Управляет экспортом OpenTelemetry.[ref: repo:src/bioetl/config/models/models.py] |
| `exporter` | `str \| None` | `null` | Тип экспортера (`jaeger`, `otlp`, `console`).[ref: repo:src/bioetl/config/models/models.py] |
| `endpoint` | `str \| None` | `null` | URL конечной точки телеметрии.[ref: repo:src/bioetl/config/models/models.py] |
| `sampling_ratio` | `PositiveFloat` | `1.0` | Доля выборки трасс.[ref: repo:src/bioetl/config/models/models.py] |

## 3. Валидация и отчёт об ошибках

Валидация выполняется Pydantic-уровнем и Pandera-контрактом. Любой лишний ключ запрещён (все модели объявлены с `extra="forbid"`), поэтому YAML с неизвестным полем упадёт на загрузке.[ref: repo:src/bioetl/config/models/models.py†L15-L388]

### 3.1 Примеры ошибок

1. Неверная версия схемы:

```text
1 validation error for PipelineConfig
version
  Input should be 1 [type=literal_error, input_value=2, input_type=int]
    For further information visit https://errors.pydantic.dev/2.12/v/literal_error
```

【c46791†L1-L4】

1. Несогласованные ключи сортировки:

```text
1 validation error for PipelineConfig
determinism
  Value error, determinism.sort.ascending must be empty or match determinism.sort.by length [type=value_error, input_value={'sort': {'by': ['assay_i...ending': [True, False]}}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.12/v/value_error
```

【215e6c†L1-L4】

## 4. Слои и алгоритм мерджа

Мердж слоёв выполняется в три шага:

```text
base.yaml (общий профиль через `<<: !include ../profiles/base.yaml`)
        ↓
дополнительные include/merge (sources.yaml, chembl.yaml, локальные overrides)
        ↓
основной pipeline.yaml (локальные поля) + `extends` (если указаны)
        ↓
CLI `--set` overrides (глубокий merge по ключам)
        ↓
переменные окружения BIOETL__/BIOACTIVITY__ (наивысший приоритет)
```

Псевдокод соответствует `read_pipeline_config`: профили из `extends` обрабатываются рекурсивно, затем применяется основной YAML, после чего последовательно накладываются CLI-override и env-overrides, и только потом выполняется `PipelineConfig.model_validate()`.[ref: repo:docs/configs/00-typed-configs-and-profiles.md@refactoring_001][ref: config-loader]

## 5. Профили `base.yaml` и `determinism.yaml`

### 5.1 Базовый профиль

```yaml
# configs/defaults/base.yaml
<<: &profile_common
  version: 1
  runtime:
    parallelism: 4
    chunk_rows: 100000
    dry_run: false
    seed: 42
  io:
    input:
      format: csv
      encoding: utf-8
      header: true
      path: null
    output:
      format: parquet
      partition_by: []
      overwrite: true
      path: null
  http:
    default:
      timeout_sec: 60.0
      connect_timeout_sec: 15.0
      read_timeout_sec: 60.0
      retries:
        total: 5
        backoff_multiplier: 2.0
        backoff_max: 60.0
        statuses: [408, 429, 500, 502, 503, 504]
      rate_limit:
        max_calls: 10
        period: 1.0
      rate_limit_jitter: true
      headers:
        User-Agent: "BioETL/1.0 (UnifiedAPIClient)"
        Accept: "application/json"
        Accept-Encoding: "gzip, deflate"
  cache:
    enabled: true
    directory: http_cache
    ttl: 86400
  paths:
    input_root: data/input
    output_root: data/output
    cache_root: .cache
  materialization:
    root: data/output
    default_format: parquet
  fallbacks:
    enabled: true
  determinism:
    enabled: true
    hash_policy_version: "1.0.0"
    float_precision: 6
    datetime_format: iso8601
    column_validation_ignore_suffixes: [_scd, _temp, _meta, _tmp]
    sort:
      by: []
      ascending: []
      na_position: last
    column_order: []
    serialization:
      csv:
        separator: ","
        quoting: ALL
        na_rep: ""
      booleans: ["True", "False"]
      nan_rep: "NaN"
    hashing:
      algorithm: sha256
      row_fields: []
      business_key_fields: []
      exclude_fields: [generated_at, run_id]
      business_key_column: hash_business_key
      row_hash_column: hash_row
      business_key_schema:
        dtype: string
        length: 64
        nullable: false
      row_hash_schema:
        dtype: string
        length: 64
        nullable: false
    environment:
      timezone: UTC
      locale: C
    write:
      strategy: atomic
    meta:
      location: sibling
      include_fields: []
      exclude_fields: []
  validation:
    strict: true
    coerce: true
  logging:
    level: INFO
    format: json
    with_timestamps: true
    context_fields: [pipeline, run_id]
  telemetry:
    enabled: true
    exporter: jaeger
    endpoint: null
    sampling_ratio: 1.0
```

【F:configs/defaults/base.yaml†L1-L93】

### 5.2 Профиль детерминизма

```yaml
# configs/defaults/determinism.yaml
determinism:
  enabled: true
  hash_policy_version: "1.0.0"
  float_precision: 6
  datetime_format: "iso8601"
  column_validation_ignore_suffixes:
    - "_scd"
    - "_temp"
    - "_meta"
    - "_tmp"
  sort:
    by: []
    ascending: []
    na_position: "last"
  column_order: []
  serialization:
    csv:
      separator: ","
      quoting: "ALL"
      na_rep: ""
    booleans: ["True", "False"]
    nan_rep: "NaN"
  hashing:
    algorithm: "sha256"
    row_fields: []
    business_key_fields: []
    exclude_fields: ["generated_at", "run_id"]
  environment:
    timezone: "UTC"
    locale: "C"
  write:
    strategy: "atomic"
  meta:
    location: "sibling"
    include_fields: []
    exclude_fields: []
```

【F:configs/defaults/determinism.yaml†L1-L49】

## 6. Пример итогового конфига

```yaml
# configs/pipelines/activity/activity_chembl.yaml
<<: !include ../../defaults/base.yaml
<<: !include ../../defaults/determinism.yaml
<<: !include ../../defaults/chembl.yaml
<<: !include ../../defaults/validation.yaml
<<: !include ../../defaults/postprocess.yaml

pipeline:
  name: activity_chembl
  version: "1.0.0"
  owner: "Data Acquisition Team"

io:
  output:
    partition_by:
      - assay_chembl_id

sources:
  chembl:
    batch_size: 20
    parameters:
      base_url: https://www.ebi.ac.uk/chembl/api/data
      max_url_length: 2000
      select_fields:
        - activity_id
        - assay_chembl_id
        - molecule_chembl_id
        - standard_value

determinism:
  sort:
    by: [assay_chembl_id, molecule_chembl_id, activity_id]
    ascending: [true, true, true]
    na_position: last
  hashing:
    business_key_fields: [activity_id, row_subtype, row_index]

validation:
  schema_out: bioetl.schemas.activity.activity_chembl.ActivitySchema
```

Вместо `extends` общий профиль подключается через `<<: !include`. Добавление локальных секций (`sources.chembl`, `determinism`, `validation`) происходит поверх общей карты `profile_common`, а списки объединяются стандартным YAML merge-key.

## 7. Переопределения через CLI и переменные окружения

### 7.1 CLI `--set`

```bash
python -m bioetl.cli.main activity \
  --config configs/pipelines/activity/activity_chembl.yaml \
  --set http.default.timeout_sec=90 \
  --set determinism.sort.by='["activity_id"]'
```

### 7.2 Переменные окружения

```bash
export BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=120
export BIOETL__SOURCES__CHEMBL__PARAMETERS__endpoint="https://chembl/api"
export BIOETL__DETERMINISM__FLOAT_PRECISION=4
```

Переменные окружения разбиваются по `__`, приводятся к нижнему регистру и глубоко мерджатся поверх CLI-override. Они имеют высший приоритет в цепочке.[ref: repo:docs/configs/00-typed-configs-and-profiles.md@refactoring_001]

## 8. Тест-план конфигураций

1. **Unit**: проверить, что отсутствует обязательное поле `pipeline.name`, некорректный тип `http.default.timeout_sec` и неизвестный ключ в `sources.chembl` приводят к `ValidationError`.
2. **Merge**: смоделировать каскад `base.yaml → determinism.yaml → pipeline.yaml → --set → env` и убедиться, что итоговое значение соответствует приоритету.
3. **CLI Integration**: e2e-тест Typer-команды `bioetl.cli.main activity` c фиктивной конфигурацией, подтверждающий, что `cli.set_overrides` и `cli.profiles` заполняются из аргументов.
4. **Golden**: при одинаковом входе и конфигурации повторный прогон формирует идентичный `PipelineConfig` и совпадающие хеши артефактов.

## 9. Рекомендации по дальнейшему развитию (опционально)

- Добавить строгие модели для конкретных источников (`ChemblSourceConfig`, `PubChemSourceConfig`) вместо общего словаря `parameters`.
- Ввести генератор документации, который автоматически строит таблицы по Pydantic-моделям и обновляет этот документ.

[ref: base-config]: repo:src/bioetl/config/models/models.py
[ref: column-order-validation]: repo:src/bioetl/config/models/models.py†L381-L388
[ref: config-loader]: repo:src/bioetl/config/loader.py@refactoring_001
