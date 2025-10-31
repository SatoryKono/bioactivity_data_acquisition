# Структура конфигураций {#configs}

## Общие принципы {#config-principles}
- Все конфиги MUST наследоваться через `extends` и валидироваться
  [`PipelineConfig`][ref: repo:src/bioetl/configs/models.py@test_refactoring_32].
- Ключ `sources` использует строгий тип [`Source`][ref: repo:src/bioetl/configs/models.py@test_refactoring_32],
  запрещающий неизвестные поля.
- Значения `env:VAR` и `${VAR}` MUST указывать на существующие переменные окружения;
  разрешение выполняется валидатором `resolve_contact_secrets`.
- Профили MAY расширять `determinism.yaml`, `cache.yaml` и другие includes.

## Основная структура {#config-structure}

```yaml
extends:
  - base.yaml
  - includes/determinism.yaml
pipeline:
  name: activity
  entity: activity
materialization:
  pipeline_subdir: "activity"
sources:
  chembl:
    enabled: true
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    batch_size: 20
    max_url_length: 2000
```
- `base.yaml` определяет пути, determinism и QC по умолчанию
  ([ref: repo:src/bioetl/configs/base.yaml@test_refactoring_32]).
- `includes/determinism.yaml` задаёт сортировку и порядок столбцов.
- Дополнительные include-файлы (например `chembl_source.yaml`) подмешивают общие
  параметры HTTP.

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

## Наследование и переопределения {#inheritance}
- Конфиг пайплайна MAY расширять несколько include-файлов; последний wins.
- Ключи `determinism.sort.by` и `determinism.column_order` MUST задаваться для
  детерминизма итоговых CSV.
- CLI флаг `--mode` MAY переопределять `pipeline.mode`; список валидных значений
  определяется в `PipelineCommandConfig`
  ([ref: repo:src/scripts/__init__.py@test_refactoring_32]).

## Инварианты {#config-invariants}
- `sources.*.headers.User-Agent` SHOULD содержать идентификатор пайплайна и версию.
- При отсутствии обязательного секрета валидатор MUST бросать `ValueError`, что
  предотвращает запуск без `env` переменных.
- `determinism.sort.by` MUST ссылаться на столбцы, присутствующие в схеме
  (см. [`ActivitySchema`][ref: repo:src/bioetl/schemas/activity.py@test_refactoring_32]).
