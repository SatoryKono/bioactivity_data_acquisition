# 01. Профили конфигурации

> **Статус**: актуально для ветки `@test_refactoring_32`. Все примеры синхронизированы с `configs/profiles/`.

## 1. Базовый профиль (`base.yaml`)

Общий профиль подключается в каждом pipeline через `<<: !include ../../profiles/base.yaml`. Он определяет:

- секции `runtime`, `io`, `http`, `logging`, `telemetry` с детерминированными значениями по умолчанию;
- гарантию детерминизма (`determinism`) с едиными колонками `hash_business_key` и `hash_row`;
- политику путей, кэшей и форматирования CSV.

```yaml
<<: &profile_common
  version: 1
  runtime:
    parallelism: 4
    chunk_rows: 100000
  io:
    input: {format: csv, encoding: utf-8, header: true}
    output: {format: parquet, partition_by: [], overwrite: true}
  logging:
    level: INFO
    format: json
  telemetry:
    enabled: true
    exporter: jaeger
```

## 2. Профиль источников (`sources.yaml`)

`configs/profiles/sources.yaml` хранит повторно используемые определения внешних API. Пример:

```yaml
sources:
  chembl_common: &chembl_common
    description: "ChEMBL REST API"
    parameters:
      base_url: https://www.ebi.ac.uk/chembl/api/data
      pagination: {page_size: 1000, until_exhausted: true}
      params: {format: json}
  uniprot: &uniprot
    http:
      rate_limit: {max_calls: 2, period: 1.0}
    parameters:
      base_url: https://rest.uniprot.org
      cache: {enabled: true}
```

Чтобы избежать попадания служебных якорей (`chembl_common`, `uniprot`) в итоговый `PipelineConfig`, профиль подключается через `<<: !include` и затем ключи-анкоры перебиваются локальными определениями.

## 3. Оверлеи пайплайнов

Каждый pipeline начинается с набора include-ов:

```yaml
<<: !include ../../profiles/base.yaml
<<: !include ../../profiles/determinism.yaml
<<: !include ../../profiles/chembl.yaml
<<: !include ../../profiles/validation.yaml
```

Далее описываются только различия:

- `sources.chembl.batch_size`, `parameters.select_fields`;
- `determinism.sort` и списки бизнес-ключей;
- `io.output.partition_by` (например, `year` для документов);
- специфичные блоки (`chembl.activity.enrich`, `transform.arrays_to_header_rows`).

## 4. Локальные оверлеи (`base.local.yaml`)

Для разработки доступен профиль `base.local.yaml`. Он не подключается автоматически, но может быть передан через `--profile` или `extends`. Содержимое переключает пайплайны в щадящий режим:

```yaml
runtime:
  parallelism: 1
  chunk_rows: 20000
  dry_run: true
io:
  output:
    path: data/output/local
logging:
  level: DEBUG
telemetry:
  enabled: false
```

## 5. Рекомендации

1. **Не изменять** базовые профили без синхронизации документации и golden-тестов.
2. При добавлении новой сущности добавляйте профили в `profiles/` и подключайте их через `<<: !include`.
3. Профили должны оставаться idempotent: отключение профиля не должно ломать загрузку других пайплайнов.

