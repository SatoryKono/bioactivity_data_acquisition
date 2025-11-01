# Структура конфигураций {#configs}

## Общие принципы {#config-principles}

- Все конфиги MUST наследоваться через `extends` и валидироваться

  [`PipelineConfig`][ref: repo:src/bioetl/configs/models.py@test_refactoring_32].

- Ключ `sources` использует строгий тип [`Source`][ref: repo:src/bioetl/configs/models.py@test_refactoring_32],

  запрещающий неизвестные поля.

- Значения `env:VAR` и `${VAR}` MUST указывать на существующие переменные окружения;

  разрешение выполняется валидатором `resolve_contact_secrets`.

- Профили MAY расширять `determinism.yaml`, `cache.yaml` и другие includes.

## Модель конфигурации {#config-model}

`PipelineConfig` описывает пайплайн, источники, материализацию, CLI-опции и
детерминизм. Pydantic-модели валидируют значения и разрешают переменные
окружения через `env:` или `${VAR:default}`.[ref: repo:src/bioetl/config/models.py@test_refactoring_32]

| Раздел | Назначение | Инварианты |
| --- | --- | --- |
| `pipeline` | Имя, сущность, версия, release scope | `entity` MUST быть зарегистрирована в `schema_registry` |
| `http` | Таймауты, ретраи, rate limit | `retries.total` ≥ 0, `statuses` перечисляют retriable коды |
| `sources` | Настройки клиентов (base_url, headers, api_key) | `enabled=false` отключает источник, остальные ключи игнорируются |
| `determinism` | Сортировка, порядок колонок, контроль хешей | `column_order` MUST включать бизнес-колонки |
| `materialization` | Папки, форматы, наборы данных | Каждый dataset имеет расширение и имя файла |
| `qc` | Пороговые значения метрик | `severity_threshold` задаёт минимальный уровень эскалации |
| `cli` | Опции по умолчанию (`mode`, `dry_run`) | `default_config` указывает путь до YAML |

## Основная структура {#config-structure}

```yaml
extends:

  - base.yaml
  - includes/determinism.yaml
  - includes/chembl_source.yaml

pipeline:
  name: activity
  entity: activity
materialization:
  pipeline_subdir: "activity"
sources:
  chembl:
    enabled: true
    base_url: "<https://www.ebi.ac.uk/chembl/api/data>"
    batch_size: 20
    max_url_length: 2000
    headers:
      User-Agent: "bioetl-activity-pipeline/1.0"

determinism:
  sort:
    by: ["activity_id"]
    ascending: [true]
```

- `base.yaml` определяет пути, determinism и QC по умолчанию

  ([ref: repo:src/bioetl/configs/base.yaml@test_refactoring_32]).

- `includes/determinism.yaml` задаёт сортировку и порядок столбцов.
- Дополнительные include-файлы (например `chembl_source.yaml`) подмешивают общие

  параметры HTTP.

[ref: repo:src/bioetl/configs/pipelines/chembl/activity.yaml@test_refactoring_32]

Комментарии:

- `batch_size` ограничивает размер батча для `ActivityRequestBuilder`.
- Пользовательский `User-Agent` MUST идентифицировать приложение.
- `determinism.sort` обеспечивает стабильно отсортированный CSV.

## Наследование и include {#inheritance-include}

- `extends` подключает базовые YAML (`../base.yaml`, includes для общих настроек).
- Файл `includes/chembl_source.yaml` задаёт стандартные параметры для ChEMBL

  (base_url, headers, rate limit jitter).[ref: repo:src/bioetl/configs/includes/chembl_source.yaml@test_refactoring_32]

- `includes/determinism.yaml` централизует настройки сортировки, игнорируемых

  QC-файлов и формат дат.[ref: repo:src/bioetl/configs/includes/determinism.yaml@test_refactoring_32]

- Конфиг пайплайна MAY расширять несколько include-файлов; последний wins.
- Ключи `determinism.sort.by` и `determinism.column_order` MUST задаваться для

  детерминизма итоговых CSV.

## Профили {#profiles}

### dev.yaml {#profile-dev}

```yaml
extends:

  - ../base.yaml
  - ../includes/cache.yaml

pipeline:
  name: "dev"
  entity: "development"
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
qc:
  severity_threshold: "error"
```

- Использует мягкие ретраи и малый TTL кеша, подходит для локальной отладки.
- Параметры HTTP наследуются в каждый пайплайн через `PipelineConfig.http.global`.

### prod.yaml {#profile-prod}

```yaml
extends:

  - ../base.yaml
  - ../includes/cache.yaml

pipeline:
  name: "prod"
  entity: "production"
http:
  global:
    timeout_sec: 60.0
    retries:
      total: 5
      backoff_multiplier: 2.0
      backoff_max: 120.0
    rate_limit:
      max_calls: 5
      period: 15.0
cache:
  ttl: 86400
  release_scoped: true
qc:
  severity_threshold: "error"
```

- Предназначен для production: длительные таймауты и кеш на сутки.
- `release_scoped` MUST быть true, чтобы различать версии ChEMBL.

## Источники и профили {#sources-profiles}

`SourceConfig` и `TargetSourceConfig` расширяют базовый набор полей, добавляя
rate limit, кэш и circuit breaker. API ключи разрешаются из окружения и MUST
быть заданы для защищённых источников (например, `IUPHAR_API_KEY`).[ref: repo:src/bioetl/config/models.py@test_refactoring_32]

### Пример: target {#example-target}

```yaml
extends:

  - ../base.yaml
  - ../includes/determinism.yaml
  - ../includes/chembl_source.yaml

http:
  chembl:
    timeout_sec: 45.0
    retries:
      statuses: [404, 408, 409, 425, 429, 500, 502, 503, 504]
  iuphar:
    rate_limit:
      max_calls: 6
      period: 1.0
sources:
  chembl:
    stage: primary
    fallback_strategies: ["cache", "partial_retry", "network", "timeout", "5xx"]
  uniprot:
    enabled: true
    cache_enabled: true
    cache_ttl: 21600
  iuphar:
    enabled: true
    api_key: "${IUPHAR_API_KEY}"
materialization:
  pipeline_subdir: "target"
  stages:
    gold:
      datasets:
        targets:
          formats:
            parquet: "targets_final.parquet"
```

[ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32]

Комментарии:

- `fallback_strategies` определяют порядок деградации клиента; значение MUST

  принадлежать множеству `SUPPORTED_FALLBACK_STRATEGIES`.

- `cache_enabled` и `cache_ttl` включают release-scoped кэш согласно `base.yaml`.
- `materialization.stages` задаёт имена файлов для silver/gold/QC уровней.

## CLI и runtime {#cli-runtime}

- CLI опции (`mode`, `dry_run`, `validate_columns`) подмешиваются через Typer и

  отражены в `PipelineBase.runtime_options`.[ref: repo:src/bioetl/cli/command.py@test_refactoring_32]

- Переопределение значений возможно через `--set key=value`, который разбирается

  функцией `parse_cli_overrides`.[ref: repo:src/bioetl/config/loader.py@test_refactoring_32]

- CLI флаг `--mode` MAY переопределять `pipeline.mode`; список валидных значений

  определяется в `PipelineCommandConfig`
  ([ref: repo:src/scripts/__init__.py@test_refactoring_32]).

## Инварианты {#config-invariants}

- `sources.*.headers.User-Agent` SHOULD содержать идентификатор пайплайна и версию.
- При отсутствии обязательного секрета валидатор MUST бросать `ValueError`, что

  предотвращает запуск без `env` переменных.

- `determinism.sort.by` MUST ссылаться на столбцы, присутствующие в схеме

  (см. [`ActivitySchema`][ref: repo:src/bioetl/schemas/activity.py@test_refactoring_32]).

- YAML MUST иметь LF окончания строк и кодировку UTF-8.
- Все пути в `materialization` относительны к `paths.output_root` из базовой

  конфигурации.[ref: repo:src/bioetl/configs/base.yaml@test_refactoring_32]

- `determinism.column_order` SHOULD включать хеши и метаданные первыми колонками.
- Профили обязаны объявлять `pipeline.version`; обновление схемы требует записи в

  CHANGELOG.
