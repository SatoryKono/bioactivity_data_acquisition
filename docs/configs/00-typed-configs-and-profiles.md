# Specification: Typed Configurations and Profiles

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

This document provides a comprehensive specification for the `bioetl` configuration system, based on the implementation in `[ref: repo:src/bioetl/config/loader.py@refactoring_001]`.

## 1. Обзор и цели

Типобезопасный объект `PipelineConfig` формирует единый контракт между CLI и пайплайнами: Typer-команда при запуске автоматически подмешивает профили `base.yaml` и `determinism.yaml`, затем валидирует объединённые данные строго по модели `PipelineConfig`.[ref: repo:README.md†L18-L29][ref: repo:src/bioetl/configs/models.py†L355-L389] Благодаря Pydantic-вёрстке (`extra="forbid"`) любой неизвестный ключ приводит к немедленной ошибке, а кросс-полевые инварианты (например, согласованность сортировки) проверяются валидаторами.[ref: repo:src/bioetl/configs/models.py†L15-L389]

## 2. Структура и типы `PipelineConfig`

### 2.1 Корневые ключи

| Section | Key | Type | Required | Default | Description |
| --- | --- | --- | --- | --- | --- |
| `PipelineConfig` | `version` | `Literal[1]` | Yes | — | Версия схемы конфигурации.[ref: repo:src/bioetl/configs/models.py†L360-L362] |
| `PipelineConfig` | `extends[]` | `Sequence[str]` | No | `[]` | Профили, которые мерджатся перед основным YAML.[ref: repo:src/bioetl/configs/models.py†L363-L366] |
| `PipelineConfig` | `pipeline` | `PipelineMetadata` | Yes | — | Метаданные пайплайна.[ref: repo:src/bioetl/configs/models.py†L349-L352][ref: repo:src/bioetl/configs/models.py†L355-L379] |
| `PipelineConfig` | `http` | `HTTPConfig` | Yes | — | HTTP-профили и базовые настройки клиентов.[ref: repo:src/bioetl/configs/models.py†L80-L91] |
| `PipelineConfig` | `cache` | `CacheConfig` | No | см. таблицу ниже | Параметры HTTP-кэша.[ref: repo:src/bioetl/configs/models.py†L94-L101] |
| `PipelineConfig` | `paths` | `PathsConfig` | No | см. таблицу ниже | Каталоги ввода/вывода.[ref: repo:src/bioetl/configs/models.py†L104-L111] |
| `PipelineConfig` | `determinism` | `DeterminismConfig` | No | см. таблицу ниже | Политика детерминизма выгрузок.[ref: repo:src/bioetl/configs/models.py†L241-L279] |
| `PipelineConfig` | `materialization` | `MaterializationConfig` | No | см. таблицу ниже | Настройки записи артефактов.[ref: repo:src/bioetl/configs/models.py†L114-L131] |
| `PipelineConfig` | `fallbacks` | `FallbacksConfig` | No | см. таблицу ниже | Поведение fallback-механизмов.[ref: repo:src/bioetl/configs/models.py†L134-L146] |
| `PipelineConfig` | `validation` | `ValidationConfig` | No | см. таблицу ниже | Ссылки на Pandera-схемы и строгий режим.[ref: repo:src/bioetl/configs/models.py†L282-L296] |
| `PipelineConfig` | `sources{}` | `Dict[str, SourceConfig]` | No | `{}` | Переопределения для отдельных источников.[ref: repo:src/bioetl/configs/models.py†L319-L341] |
| `PipelineConfig` | `cli` | `CLIConfig` | No | см. таблицу ниже | Захваченные значения CLI-флагов.[ref: repo:src/bioetl/configs/models.py†L299-L316] |

### 2.2 `pipeline`

| Key | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | `str` | Yes | — | Уникальное имя пайплайна.[ref: repo:src/bioetl/configs/models.py†L349-L352] |
| `version` | `str` | Yes | — | Семантическая версия реализации.[ref: repo:src/bioetl/configs/models.py†L349-L352] |
| `owner` | `str` | No | `null` | Ответственный инженер/команда.[ref: repo:src/bioetl/configs/models.py†L351-L352] |
| `description` | `str` | No | `null` | Краткое описание.[ref: repo:src/bioetl/configs/models.py†L351-L352] |

### 2.3 `http`

| Key | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `default.timeout_sec` | `PositiveFloat` | No | `60.0` | Общий таймаут запроса.[ref: repo:src/bioetl/configs/models.py†L55-L63] |
| `default.connect_timeout_sec` | `PositiveFloat` | No | `15.0` | Таймаут соединения.[ref: repo:src/bioetl/configs/models.py†L55-L63] |
| `default.read_timeout_sec` | `PositiveFloat` | No | `60.0` | Таймаут чтения сокета.[ref: repo:src/bioetl/configs/models.py†L55-L63] |
| `default.retries.*` | `RetryConfig` | No | см. подтаблицу | Политика повторов.[ref: repo:src/bioetl/configs/models.py†L64-L78] |
| `default.rate_limit.*` | `RateLimitConfig` | No | см. подтаблицу | Ограничение запросов.[ref: repo:src/bioetl/configs/models.py†L65-L69][ref: repo:src/bioetl/configs/models.py†L35-L47] |
| `default.rate_limit_jitter` | `bool` | No | `true` | Добавляет джиттер к лимитам.[ref: repo:src/bioetl/configs/models.py†L66-L69] |
| `default.headers{}` | `Mapping[str, str]` | No | см. значение | Базовые заголовки HTTP.[ref: repo:src/bioetl/configs/models.py†L70-L77] |
| `profiles.<name>` | `HTTPClientConfig` | No | `{}` | Именованные профили для источников.[ref: repo:src/bioetl/configs/models.py†L85-L91] |

**`RetryConfig`**

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `total` | `PositiveInt` | `5` | Количество повторов.[ref: repo:src/bioetl/configs/models.py†L17-L32] |
| `backoff_multiplier` | `PositiveFloat` | `2.0` | Множитель экспоненциального backoff.[ref: repo:src/bioetl/configs/models.py†L21-L24] |
| `backoff_max` | `PositiveFloat` | `60.0` | Максимальная задержка между попытками.[ref: repo:src/bioetl/configs/models.py†L25-L28] |
| `statuses[]` | `Tuple[int]` | `(408,429,500,502,503,504)` | Коды, запускающие повтор.[ref: repo:src/bioetl/configs/models.py†L29-L32] |

**`RateLimitConfig`**

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `max_calls` | `PositiveInt` | `10` | Запросов в окне.[ref: repo:src/bioetl/configs/models.py†L40-L43] |
| `period` | `PositiveFloat` | `1.0` | Длина окна в секундах.[ref: repo:src/bioetl/configs/models.py†L44-L47] |

### 2.4 Инфраструктурные блоки

| Section | Key | Default | Description |
| --- | --- | --- | --- |
| `cache` | `enabled` | `true` | Вкл./выкл. дискового кэша.[ref: repo:src/bioetl/configs/models.py†L99-L101] |
|  | `directory` | `"http_cache"` | Каталог кэша.[ref: repo:src/bioetl/configs/models.py†L99-L101] |
|  | `ttl` | `86400` | TTL записи (сек).[ref: repo:src/bioetl/configs/models.py†L99-L101] |
| `paths` | `input_root` | `"data/input"` | Базовый каталог входных данных.[ref: repo:src/bioetl/configs/models.py†L109-L111] |
|  | `output_root` | `"data/output"` | Базовый каталог выгрузок.[ref: repo:src/bioetl/configs/models.py†L109-L111] |
|  | `cache_root` | `".cache"` | Каталог временных файлов.[ref: repo:src/bioetl/configs/models.py†L109-L111] |
| `materialization` | `root` | `"data/output"` | Корень артефактов.[ref: repo:src/bioetl/configs/models.py†L119-L123] |
|  | `default_format` | `"parquet"` | Формат по умолчанию.[ref: repo:src/bioetl/configs/models.py†L119-L123] |
|  | `pipeline_subdir` | `null` | Доп. подкаталог.[ref: repo:src/bioetl/configs/models.py†L124-L127] |
|  | `filename_template` | `null` | Jinja-шаблон имени файла.[ref: repo:src/bioetl/configs/models.py†L128-L131] |
| `fallbacks` | `enabled` | `true` | Включает fallback-стратегии.[ref: repo:src/bioetl/configs/models.py†L139-L146] |
|  | `max_depth` | `null` | Ограничение глубины fallback.[ref: repo:src/bioetl/configs/models.py†L139-L146] |
| `validation` | `schema_in` | `null` | Путь к входной Pandera-схеме.[ref: repo:src/bioetl/configs/models.py†L287-L296] |
|  | `schema_out` | `null` | Путь к выходной схеме.[ref: repo:src/bioetl/configs/models.py†L291-L296] |
|  | `strict` | `true` | Требует строгого порядка колонок.[ref: repo:src/bioetl/configs/models.py†L295-L296] |
|  | `coerce` | `true` | Приводит типы в Pandera.[ref: repo:src/bioetl/configs/models.py†L295-L296] |
| `cli` | `profiles[]` | `[]` | Профили, переданные через `--profile`.[ref: repo:src/bioetl/configs/models.py†L304-L316] |
|  | `dry_run` | `false` | Флаг `--dry-run`.[ref: repo:src/bioetl/configs/models.py†L308-L316] |
|  | `limit` | `null` | Лимит записей (`--limit`).[ref: repo:src/bioetl/configs/models.py†L309-L312] |
|  | `set_overrides{}` | `{}` | Пары `--set key=value`.[ref: repo:src/bioetl/configs/models.py†L313-L316] |
| `sources.<id>` | `enabled` | `true` | Отключение источника.[ref: repo:src/bioetl/configs/models.py†L324-L341] |
|  | `description` | `null` | Описание источника.[ref: repo:src/bioetl/configs/models.py†L325-L341] |
|  | `http_profile` | `null` | Ссылка на HTTP-профиль.[ref: repo:src/bioetl/configs/models.py†L326-L333] |
|  | `http` | `HTTPClientConfig` | `null` | Inline-переопределения HTTP.[ref: repo:src/bioetl/configs/models.py†L330-L333] |
|  | `batch_size` | `null` | Размер батча для пагинации.[ref: repo:src/bioetl/configs/models.py†L334-L337] |
|  | `parameters{}` | `{}` | Произвольные параметры источника.[ref: repo:src/bioetl/configs/models.py†L338-L341] |

### 2.5 `determinism`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `true` | Включает гарантии детерминизма.[ref: repo:src/bioetl/configs/models.py†L246-L279] |
| `hash_policy_version` | `str` | `"1.0.0"` | Версионирование стратегии хеширования.[ref: repo:src/bioetl/configs/models.py†L247-L250] |
| `float_precision` | `PositiveInt` | `6` | Количество знаков после запятой при нормализации.[ref: repo:src/bioetl/configs/models.py†L251-L254] |
| `datetime_format` | `str` | `"iso8601"` | Формат сериализации дат.[ref: repo:src/bioetl/configs/models.py†L255-L258] |
| `column_validation_ignore_suffixes[]` | `Sequence[str]` | `("_scd","_temp","_meta","_tmp")` | Суффиксы, игнорируемые при проверке колонок.[ref: repo:src/bioetl/configs/models.py†L259-L262] |
| `sort.by[]` | `List[str]` | `[]` | Столбцы сортировки.[ref: repo:src/bioetl/configs/models.py†L172-L183] |
| `sort.ascending[]` | `List[bool]` | `[]` | Направление сортировки; должно совпадать по длине с `sort.by`.[ref: repo:src/bioetl/configs/models.py†L172-L279] |
| `sort.na_position` | `str` | `"last"` | Положение `NA` при сортировке.[ref: repo:src/bioetl/configs/models.py†L172-L183] |
| `column_order[]` | `Sequence[str]` | `[]` | Жёстко фиксированный порядок колонок (требует `validation.schema_out`).[ref: repo:src/bioetl/configs/models.py†L263-L267][ref: repo:src/bioetl/configs/models.py†L381-L388] |
| `serialization.csv.separator` | `str` | `","` | Разделитель CSV.[ref: repo:src/bioetl/configs/models.py†L154-L169] |
| `serialization.csv.quoting` | `str` | `"ALL"` | Стратегия кавычек.[ref: repo:src/bioetl/configs/models.py†L154-L156] |
| `serialization.csv.na_rep` | `str` | `""` | Представление `NA` в CSV.[ref: repo:src/bioetl/configs/models.py†L154-L156] |
| `serialization.booleans[]` | `Tuple[str,str]` | `("True","False")` | Строковые значения для bool.[ref: repo:src/bioetl/configs/models.py†L164-L168] |
| `serialization.nan_rep` | `str` | `"NaN"` | Представление `NaN`.[ref: repo:src/bioetl/configs/models.py†L164-L169] |
| `hashing.algorithm` | `str` | `"sha256"` | Алгоритм хеширования.[ref: repo:src/bioetl/configs/models.py†L185-L201] |
| `hashing.row_fields[]` | `Sequence[str]` | `[]` | Поля для `hash_row`.[ref: repo:src/bioetl/configs/models.py†L190-L197] |
| `hashing.business_key_fields[]` | `Sequence[str]` | `[]` | Поля бизнес-ключа.[ref: repo:src/bioetl/configs/models.py†L195-L198] |
| `hashing.exclude_fields[]` | `Sequence[str]` | `("generated_at","run_id")` | Поля, исключаемые из хешей.[ref: repo:src/bioetl/configs/models.py†L199-L201] |
| `environment.timezone` | `str` | `"UTC"` | Часовой пояс исполнения.[ref: repo:src/bioetl/configs/models.py†L205-L212] |
| `environment.locale` | `str` | `"C"` | Локаль для форматирования.[ref: repo:src/bioetl/configs/models.py†L205-L212] |
| `write.strategy` | `str` | `"atomic"` | Стратегия записи артефактов.[ref: repo:src/bioetl/configs/models.py†L214-L220] |
| `meta.location` | `str` | `"sibling"` | Расположение `meta.yaml`.[ref: repo:src/bioetl/configs/models.py†L222-L238] |
| `meta.include_fields[]` | `Sequence[str]` | `[]` | Поля, обязанные попасть в `meta`.[ref: repo:src/bioetl/configs/models.py†L231-L234] |
| `meta.exclude_fields[]` | `Sequence[str]` | `[]` | Поля, исключаемые перед хешированием.[ref: repo:src/bioetl/configs/models.py†L235-L238] |

Дополнительные инварианты:

- `determinism.sort.ascending` должно быть пустым или совпадать по длине с `determinism.sort.by`; нарушение приводит к ValueError.[ref: repo:src/bioetl/configs/models.py†L274-L279]
- Если задан `determinism.column_order`, необходимо указать `validation.schema_out`, иначе валидация отклонит конфиг.[ref: repo:src/bioetl/configs/models.py†L381-L388]

## 3. Валидация и отчёт об ошибках

Валидация выполняется Pydantic-уровнем и Pandera-контрактом. Любой лишний ключ запрещён (все модели объявлены с `extra="forbid"`), поэтому YAML с неизвестным полем упадёт на загрузке.[ref: repo:src/bioetl/configs/models.py†L15-L388]

### 3.1 Примеры ошибок

1. Неверная версия схемы:

```
1 validation error for PipelineConfig
version
  Input should be 1 [type=literal_error, input_value=2, input_type=int]
    For further information visit https://errors.pydantic.dev/2.12/v/literal_error
```
【c46791†L1-L4】

2. Несогласованные ключи сортировки:

```
1 validation error for PipelineConfig
determinism
  Value error, determinism.sort.ascending must be empty or match determinism.sort.by length [type=value_error, input_value={'sort': {'by': ['assay_i...ending': [True, False]}}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.12/v/value_error
```
【215e6c†L1-L4】

## 4. Слои и алгоритм мерджа

Мердж слоёв выполняется в три шага:

```text
extends (base.yaml, determinism.yaml, дополнительные профили)
        ↓
основной pipeline.yaml (после развёртывания !include)
        ↓
CLI --set overrides (глубокий merge по ключам)
        ↓
переменные окружения BIOETL__/BIOACTIVITY__ (имеют высший приоритет)
```

Псевдокод соответствует `load_config`: профили из `extends` обрабатываются рекурсивно, затем применяется основной YAML, после чего последовательно накладываются CLI-override и env-overrides, и только потом выполняется `PipelineConfig.model_validate()`.[ref: repo:docs/configs/00-typed-configs-and-profiles.md@refactoring_001][ref: repo:src/bioetl/config/loader.py@refactoring_001]

## 5. Профили `base.yaml` и `determinism.yaml`

### 5.1 Базовый профиль

```yaml
# configs/profiles/base.yaml
version: 1
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
  directory: "http_cache"
  ttl: 86400
paths:
  input_root: "data/input"
  output_root: "data/output"
  cache_root: ".cache"
materialization:
  root: "data/output"
  default_format: "parquet"
fallbacks:
  enabled: true
validation:
  strict: true
  coerce: true
```
【F:configs/profiles/base.yaml†L1-L48】

### 5.2 Профиль детерминизма

```yaml
# configs/profiles/determinism.yaml
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
【F:configs/profiles/determinism.yaml†L1-L49】

## 6. Пример итогового конфига

```yaml
# configs/pipelines/chembl/activity.yaml
extends:
  - ../profiles/base.yaml
  - ../profiles/determinism.yaml
  - ../profiles/network.yaml

pipeline:
  name: activity
  version: "2.1.0"
  owner: "chembl-team"

http:
  default:
    timeout_sec: 45.0  # override базового профиля
  profiles:
    chembl:
      timeout_sec: 30.0
      retries:
        total: 7

sources:
  chembl:
    http_profile: chembl
    batch_size: 25
    parameters:
      endpoint: "/activity.json"
      filters: !include ../fragments/activity_filters.yaml

determinism:
  sort:
    by: ["assay_id", "activity_id"]
    ascending: [true, true]
  hashing:
    row_fields: ["activity_id", "standard_value"]
    business_key_fields: ["activity_id"]

validation:
  schema_out: "bioetl.schemas.chembl.activity.ActivityOutputSchema"
```

В этом примере `!include` подтягивает фрагмент с параметрами запроса, `chembl` использует именованный HTTP-профиль, а сортировка и хеширование переопределяют стандартный профиль.

## 7. Переопределения через CLI и переменные окружения

### 7.1 CLI `--set`

```
python -m bioetl.cli.main activity \
  --config configs/pipelines/chembl/activity.yaml \
  --set http.default.timeout_sec=90 \
  --set determinism.sort.by='["activity_id"]'
```

### 7.2 Переменные окружения

```
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
