# Ввод и вывод: договоры, схемы и конфиг

> **Обновление:** Структура `src/bioetl/sources/` остаётся канонической для внешних источников данных. Модульные реализации ChEMBL находятся в `src/bioetl/sources/chembl/<entity>/`, а файлы `src/bioetl/pipelines/*.py` сохранены как совместимые прокси, которые реэкспортируют новые пайплайны.

## 1) Ввод (Input Contract)

Источник истины ввода — официальные REST-интерфейсы провайдеров. Извлечение выполняется только через client/ с учётом лимитов, ретраев и этикета API.

`extract()` каждого пайплайна возвращает табличное представление данных — строго `pd.DataFrame` с именованными колонками. Тип контракта закреплён в базовом классе пайплайна, поэтому все реализации наследников (`document`, `activity`, `target`, `testitem`, …) выдают именно таблицу, даже если промежуточно работают со словарями/JSON.【F:src/bioetl/pipelines/base.py†L785-L880】

`transform()` и `validate()` обязаны принимать и возвращать `pd.DataFrame`, обеспечивая единый поток данных между стадиями, а `export()` фиксирует результат через `UnifiedOutputWriter` c учётом детерминизма и QC-метрик.【F:src/bioetl/pipelines/base.py†L785-L880】

Полученный датафрейм сразу связывается со схемой из централизованного `schema_registry` (фасад `bioetl.core.unified_schema`). Реестр фиксирует `schema_id`, `schema_version`, `column_order`, `na_policy` и `precision_policy`, что затем попадает в метаданные. Fail-fast проверку дрейфа колонок выполняет `PipelineBase.export()`, сравнивающая `df.columns` с порядком из реестра до записи файлов.【F:src/bioetl/core/unified_schema.py†L19-L82】【F:src/bioetl/schemas/registry.py†L22-L109】【F:src/bioetl/pipelines/base.py†L826-L855】

Форматы ответа: JSON по умолчанию; для NCBI E-utilities поддерживается XML/Medline (efetch, esummary), что отражается в парсере. Идентификация клиента для Crossref/OpenAlex должна включать mailto и корректный User-Agent — это влияет на квоты и «polite pool».

Пагинация (Page/Size, Cursor, Offset/Limit, Token) реализуется стратегиями; порядок результатов фиксируется и документируется per-источник.

Сырые ответы API могут храниться для отладки, но в публичный контракт попадает таблица. Минимальная форма записи, которую клиент отдаёт в parser/ (RawRecord), остаётся прежней:

```json
{
  "_source": "crossref|pubmed|openalex|...",
  "_fetched_at": "2025-10-31T08:00:00Z",
  "_request": {
    "request_id": "uuid-...",
    "endpoint": "https://api.example/...",
    "page": 3,
    "cursor": "AoJ...", 
    "status": 200,
    "retry_count": 1,
    "elapsed_ms": 1234
  },
  "payload": { /* неизменённый ответ API (JSON/XML to-be-parsed) */ }
}
```

### Логирование HTTP-запросов

Все сетевые вызовы проходят через `UnifiedAPIClient`. Контекст `_RequestRetryContext` автоматически логирует каждую попытку (`retrying_request`, `retrying_request_exception`) с номером попытки, `status_code`, `Retry-After`, рассчитанными задержками и текстом ошибки, а при полном исчерпании ретраев пишет `request_failed_after_retries` вместе с признаками тела (`data_present`/`json_present`). Такой же слой фиксирует принудительные остановки (`request_exception_giveup`) и успешные частичные ретраи. Благодаря этому в логах оказываются URL, HTTP-метод, параметры запроса и метаданные ожиданий без дублирования в коде адаптеров.【F:src/bioetl/core/api_client.py†L317-L463】

### Примечания по ключевым источникам:

**Crossref**: works JSON с полями DOI, заголовки, авторы, ссылки; ограничение строк (rows) и фильтры. Указывать mailto.

**OpenAlex**: сущность works с богатой моделью (идентификаторы, авторы, аффилиации, ссылки, поля select, курсорная пагинация). Рекомендуется mailto для повышения лимита.

**NCBI E-utilities / PubMed**: конвейер ESearch→(EPost)→EFetch для UID→записи; фиксированный URL-синтаксис.

**Semantic Scholar**: параметр fields управляет составом ответа.

**UniProt**: REST uniprotkb с query/fields/return_fields.

**PubChem PUG REST/PUG-View**: JSON/SD/XML; чёткий URL-синтаксис; аннотации через PUG-View.

**ChEMBL**: веб-сервисы .../api/data, официальные интерактивные доки и Python-клиент.

**IUPHAR/BPS GtoP**: REST JSON для targets/ligands/interactions.

## 2) Вывод (Output Contract)

Цель — детерминированные артефакты с воспроизводимыми байт-в-байт результатами.

Файлы данных: нормализованные таблицы по сущностям (см. схемы ниже), форматы CSV и/или Parquet. Порядок столбцов фиксирован, сортировка по бизнес-ключам, одинаковые правила сериализации чисел/дат/строк.

Контроль целостности: хеши строк и наборов бизнес-ключей (SHA256 из [src/bioetl/core/hashing.py](../src/bioetl/core/hashing.py)) фиксируются в метаданых экспорта. Активный алгоритм определяется конфигурацией `determinism.hash_algorithm` (см. [src/bioetl/configs/includes/determinism.yaml](../src/bioetl/configs/includes/determinism.yaml)) и по умолчанию равен `"sha256"`, что синхронизирует политику сериализации и проверку целостности в пайплайне. Подробно описано в [docs/requirements/00-architecture-overview.md](../docs/requirements/00-architecture-overview.md).[^determinism-hash]

Атомарная запись: запись во временный файл на той же ФС и атомарная замена целевого файла (replace/move_atomic). На POSIX это rename/replace, на Windows — соответствующий безопасный вызов; библиотека atomicwrites документирует детали.

Структура каталога вывода (рекомендованная):

```
data/<source>/
  documents.csv
  targets.csv
  assays.csv
  testitems.csv
  activities.csv
  activity_fact.csv
  meta.yaml
```

### Обязательные поля meta.yaml

`UnifiedOutputWriter._write_metadata()` формирует `meta.yaml`, объединяя обязательные атрибуты пайплайна и вычисленные артефакты.【F:src/bioetl/core/output_writer.py†L992-L1043】 Минимальный контракт включает:

- `run_id` — идентификатор запуска, совпадает с `PipelineBase.run_id` и попадает в checksum-namespace.【F:src/bioetl/core/output_writer.py†L992-L1001】
- `pipeline_version`, `source_system`, `chembl_release` — версия пайплайна, источник данных и релиз ChEMBL, передаваемые через `OutputMetadata`. Для ad-hoc тестов они берутся из `config.pipeline` и проверяются интеграционным сценарием.【F:src/bioetl/core/output_writer.py†L992-L1000】【F:tests/integration/pipelines/test_extended_mode_outputs.py†L109-L115】
- `extraction_timestamp` — временная метка генерации в формате ISO 8601 с таймзоной (UTC `Z` или `+00:00`).【F:src/bioetl/core/output_writer.py†L997-L998】【F:tests/integration/pipelines/test_extended_mode_outputs.py†L122-L123】
- `row_count`, `column_count`, `column_order` — фактическая форма датасета после применения детерминизма.【F:src/bioetl/core/output_writer.py†L998-L1001】【F:tests/integration/pipelines/test_extended_mode_outputs.py†L118-L120】
- `file_checksums` — словарь SHA256 по всем обязательным и дополнительным файлам (`dataset`, `quality_report`, дополнительные таблицы, QC-отчёты). Контрольная сумма вычисляется для каждого файла, который попал в артефакты.【F:src/bioetl/core/output_writer.py†L745-L768】
- `config_hash`, `git_commit`, `sources` — слепок конфигурации, Git-коммит (может быть `null`) и список включённых источников. Источники сортируются детерминированно.【F:src/bioetl/core/output_writer.py†L1002-L1004】【F:tests/integration/pipelines/test_extended_mode_outputs.py†L113-L116】
- `schema_id`, `schema_version`, `column_order_source`, `na_policy`, `precision_policy` — атрибуты из `SchemaRegistry`, фиксирующие происхождение и правила сериализации колонок (см. фасад `bioetl.core.unified_schema`).【F:src/bioetl/core/output_writer.py†L1005-L1009】【F:src/bioetl/core/unified_schema.py†L19-L82】

Дополнительно в файл автоматически добавляются:

- `hash_policy_version`, если активирована политика хеширования детерминизма.【F:src/bioetl/core/output_writer.py†L1012-L1013】
- `config_snapshot` с путём и SHA256 исходного `config.yaml`, если конфигурация прикреплена к пайплайну.【F:src/bioetl/core/output_writer.py†L1015-L1033】
- `artifacts`, содержащий пути до основного датасета, quality report, дополнительных таблиц и QC-блока. При расширенном режиме (`extended=True`) в `artifacts.qc` всегда присутствуют `correlation_report`, `summary_statistics`, `dataset_metrics`, что закреплено интеграционным тестом.【F:src/bioetl/core/output_writer.py†L1020-L1043】【F:src/bioetl/core/output_writer.py†L695-L808】【F:tests/integration/pipelines/test_extended_mode_outputs.py†L127-L130】
- `qc_summary`, `qc_metrics`, `validation_issues`, `runtime_options` — только если пайплайн их произвёл.【F:src/bioetl/core/output_writer.py†L1045-L1055】
- `lineage` — всегда присутствует блок с массивами `source_files` и `transformations`; при отсутствии пользовательских данных создаётся пустой каркас.【F:src/bioetl/core/output_writer.py†L1057-L1063】

Пример `meta.yaml`, отражающий контракт:

```yaml
run_id: "integration-test"
pipeline_version: "1.2.3"
source_system: "integration"
chembl_release: null
extraction_timestamp: "2025-10-31T13:32:28.337531+00:00"
row_count: 4
column_count: 3
column_order:
  - "numeric_a"
  - "numeric_b"
  - "category"
file_checksums:
  integration.csv: "sha256:..."
  integration_quality_report.csv: "sha256:..."
  integration_correlation_report.csv: "sha256:..."
  integration_summary_statistics.csv: "sha256:..."
  integration_dataset_metrics.csv: "sha256:..."
config_hash: "integration-hash"
git_commit: null
sources:
  - "chembl"
schema_id: null
schema_version: null
column_order_source: "dataframe"
na_policy: "allow"
precision_policy: "%.6f"
artifacts:
  dataset: "integration/datasets/integration.csv"
  quality_report: "integration/qc/integration_quality_report.csv"
  qc:
    correlation_report: "integration/qc/integration_correlation_report.csv"
    summary_statistics: "integration/qc/integration_summary_statistics.csv"
    dataset_metrics: "integration/qc/integration_dataset_metrics.csv"
lineage:
  source_files: []
  transformations: []
```

Раздел QC дополняется тремя файлами:

- `*_correlation_report.csv` — tidy-матрица корреляций `feature_x`, `feature_y`, `correlation` для всех числовых столбцов.【F:src/bioetl/core/output_writer.py†L695-L708】【F:src/bioetl/core/output_writer.py†L895-L917】
- `*_summary_statistics.csv` — транпонированный `describe(include="all")` со столбцом `column` и статистиками по типам данных.【F:src/bioetl/core/output_writer.py†L715-L937】【F:tests/integration/pipelines/test_extended_mode_outputs.py†L96-L99】
- `*_dataset_metrics.csv` — агрегированные QC-метрики (`row_count`, `null_fraction_total`, `memory_usage_bytes` и др.).【F:src/bioetl/core/output_writer.py†L731-L973】【F:tests/integration/pipelines/test_extended_mode_outputs.py†L100-L102】

**Обоснование:** Полный набор полей обеспечивает воспроизводимость, аудит конфигураций и трассировку детерминизма для downstream-консьюмеров.

## 3) Схемы данных (UnifiedSchema)

**Валидация выполняется Pandera-схемами из Schema Registry.**

### Schema Registry

Централизованный реестр Pandera-схем с версионированием. Официальная точка входа — фасад `bioetl.core.unified_schema`, предоставляющий `register_schema()`, `get_schema()` и `get_schema_metadata()` для доступа к данным реестра без прямых зависимостей от конкретных модулей схем.【F:src/bioetl/core/unified_schema.py†L19-L82】

#### Структура схемы

Каждая схема содержит:

- `schema_id`: уникальный идентификатор (например, `document.chembl`)
- `schema_version`: семантическая версия (semver: MAJOR.MINOR.PATCH)
- `column_order`: источник истины для порядка колонок

```python
class DocumentSchema(BaseSchema):
    """Схема для ChEMBL документов."""

    # Метаданные схемы
    schema_id = "document.chembl"
    schema_version = "2.1.0"

    # Порядок колонок (источник истины)
    column_order = [
        "document_chembl_id", "title", "journal", "year",
        "doi", "pmid", "hash_business_key", "hash_row"
    ]

    # Поля схемы
    document_chembl_id: str = pa.Field(str_matches=r'^CHEMBL\d+$')
    title: str = pa.Field(nullable=False)
    journal: str | None = pa.Field(nullable=True)
    year: int | None = pa.Field(ge=1800, le=2030, nullable=True)
    ...
```

#### Правила эволюции схем

**Semantic Versioning (MAJOR.MINOR.PATCH):**

| Изменение | Impact | Версия |
|-----------|--------|--------|
| Удаление колонки | Breaking | MAJOR++ |
| Переименование колонки | Breaking | MAJOR++ |
| Добавление обязательной колонки | Breaking | MAJOR++ |
| Изменение типа колонки | Breaking | MAJOR++ |
| Добавление опциональной колонки | Compatible | MINOR++ |
| Добавление constraint | Backward | MINOR++ |
| Изменение column_order | Compatible | PATCH++ |

**Матрица совместимости:**

| From | To | Compatibility | Required Actions |
|------|-----|---------------|------------------|
| 2.0.0 | 2.1.0 | ✅ Compatible | Нет |
| 2.0.0 | 3.0.0 | ⚠️ Breaking | Migration script |
| 2.1.0 | 2.0.0 | ❌ Incompatible | Downgrade запрещен |

**Fail-fast на major drift:**

При несовпадении major-версии схемы пайплайн **обязан** упасть, если включен флаг `--fail-on-schema-drift` (по умолчанию в production — включен).

Ниже минимальные поля и типы для каждой сущности. Все таблицы MUST иметь source, ingest_timestamp и стабильный бизнес-ключ.

### 3.1 documents

| поле | тип | описание |
|------|-----|----------|
| document_id | string | стабильный внутренний ID (например, нормализованный DOI/PMID) |
| doi | string? | DOI в каноническом виде |
| pmid | string? | идентификатор PubMed |
| title | string | заголовок |
| venue | string? | журнал/книга/сборник |
| year | int? | год публикации |
| authors | array<object> | список авторов {given, family, orcid?} |
| affiliations | array<object>? | {name, ror?} |
| abstract | string? | аннотация |
| urls | array<string>? | ссылки на полнотекст |
| source | string | имя источника |
| ingest_timestamp | datetime | время нормализации |

Примечания по полям см. в API Crossref/OpenAlex/Semantic Scholar.

### 3.2 targets

| поле | тип | описание |
|------|-----|----------|
| target_id | string | внутренний ID |
| uniprot_id | string? | UniProt Accession |
| gene_symbol | string? | символ гена |
| organism | string? | организм |
| target_name | string | человекочитаемое имя |
| target_type | string | GPCR, kinase, ion_channel и т.п. |
| source | string | источник |
| ingest_timestamp | datetime | время нормализации |

Справочники полей/выдач см. в UniProt и IUPHAR/BPS.

### 3.3 assays

| поле | тип | описание |
|------|-----|----------|
| assay_id | string | внутренний ID |
| assay_type | string | тип (binding, functional, phenotypic) |
| organism | string? | вид |
| cell_line | string? | клеточная линия |
| endpoint | string | измеряемый показатель |
| unit | string? | единицы |
| conditions | object? | контекст эксперимента |
| document_id | string? | связь с documents |
| source | string | источник |
| ingest_timestamp | datetime | время нормализации |

Основные поля и связности — по ChEMBL/IUPHAR.

### 3.4 testitems

| поле | тип | описание |
|------|-----|----------|
| testitem_id | string | внутренний ID |
| chembl_id | string? | ChEMBL Molecule |
| cid | int? | PubChem Compound ID |
| inchikey | string? | InChIKey |
| smiles | string? | канонический SMILES |
| preferred_name | string? | имя вещества |
| source | string | источник |
| ingest_timestamp | datetime | время нормализации |

Полезные идентификаторы и форматы описаны в PubChem/ChEMBL.

### 3.5 activities

| поле | тип | описание |
|------|-----|----------|
| activity_id | string | внутренний ID |
| assay_id | string | FK на assays |
| testitem_id | string | FK на testitems |
| relation | string? | =, <, >, ≤, ≥ |
| value | float? | численное значение |
| unit | string? | единицы |
| standard_type | string? | нормализованный тип (Ki, IC50, EC50…) |
| standard_relation | string? | нормализованный relation |
| standard_value | float? | нормализованное значение |
| standard_unit | string? | нормализованные единицы |
| source | string | источник |
| ingest_timestamp | datetime | время нормализации |

Свод по нормализации типов/единиц — по ChEMBL.

### 3.6 activity_fact (факт-таблица)

| поле | тип | описание |
|------|-----|----------|
| fact_id | string | ключ факта |
| document_id | string | FK на documents |
| target_id | string | FK на targets |
| assay_id | string | FK на assays |
| testitem_id | string | FK на testitems |
| activity_id | string | FK на activities |
| source | string | источник |
| ingest_timestamp | datetime | время нормализации |

## 4) Структура config и её валидация

Конфиг источника хранится в файле:
```
src/bioetl/configs/pipelines/<source>.yaml
```

Допускается выносить общие блоки в `src/bioetl/configs/includes/*.yaml` и подключать их через include-директивы (например, `_shared/chembl_source.yaml`). После подстановки всех include-файлов итоговый YAML автоматически валидируется объектом `PipelineConfig`; ошибки схемы или несовместимые ключи MUST прерывать запуск.

### 4.1 Pydantic-модель PipelineConfig

Валидация конфигурации построена на модели `PipelineConfig` и дочерних классах Pydantic, что исключает расхождения между документацией и реальным кодом.【F:src/bioetl/config/models.py†L691-L739】 Основные узлы:

- `pipeline` — метаданные пайплайна (`PipelineMetadata`), включающие имя, сущность и семантическую версию, прошедшую проверку по PEP 440.【F:src/bioetl/config/models.py†L321-L340】
- `http.global` — дефолтный профиль HTTP с таймаутами, ретраями и лимитами (`HttpConfig` и связанные `RetryConfig`/`RateLimitConfig`). Конфигурация задаётся словарём профилей, где ключ `global` доступен всем источникам как базовый шаблон.【F:src/bioetl/config/models.py†L44-L101】【F:src/bioetl/config/models.py†L691-L701】
- `sources` — каталог подключённых источников (`TargetSourceConfig`). Здесь описываются базовые URL, ключи API (с поддержкой `env:` ссылок), лимиты, стратегии резервирования и дополнительные заголовки.【F:src/bioetl/config/models.py†L103-L239】
- `determinism` — политика воспроизводимости (`DeterminismConfig`), фиксирующая алгоритм хеширования, сортировку, порядок колонок и исключения для валидации колонок.【F:src/bioetl/config/models.py†L248-L289】
- `materialization` — описание путей материализации артефактов (`MaterializationPaths`) с форматами, стадиями и именованием датасетов.【F:src/bioetl/config/models.py†L321-L492】

Дополнительные блоки `cache`, `paths`, `qc`, `postprocess`, `fallbacks` и `cli` также валидируются той же моделью, что гарантирует корректность конфигурации при старте пайплайна.【F:src/bioetl/config/models.py†L240-L320】【F:src/bioetl/config/models.py†L444-L671】

### 4.2 YAML-пример базового конфига

```yaml
# src/bioetl/configs/base.yaml (усечённый фрагмент)
version: 1
pipeline:
  name: "base"
  entity: "abstract"
  version: "1.0.0"
  release_scope: true

http:
  global:
    timeout_sec: 60.0
    retries:
      total: 5
      backoff_multiplier: 2.0
      backoff_max: 120.0
      statuses: [408, 425, 429, 500, 502, 503, 504]
    rate_limit:
      max_calls: 5
      period: 15.0
    headers: {}

cache:
  enabled: true
  directory: "data/cache"
  ttl: 86400
  release_scoped: true

paths:
  input_root: "data/input"
  output_root: "data/output"

materialization:
  root: "data/output"
  default_format: "parquet"
  formats:
    parquet:
      extension: ".parquet"
    csv:
      extension: ".csv"
  stages:
    gold:
      format: "parquet"
      directory: "."
      datasets:
        targets:
          filename: "targets_final"

determinism:
  sort:
    by: []
    ascending: []
  column_order: []

sources:
  pubchem:
    enabled: true
    base_url: "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    headers:
      Accept: "application/json"
      User-Agent: "bioetl-pubchem-default/1.0"

postprocess:
  qc:
    enabled: true

qc:
  enabled: true
  severity_threshold: "warning"
  thresholds: {}

cli:
  default_config: "configs/base.yaml"
  mode_choices: ["default"]
  fail_on_schema_drift: true
```

### 4.3 Include для политики детерминизма

```yaml
# src/bioetl/configs/includes/determinism.yaml
determinism:
  hash_algorithm: "sha256"
  float_precision: 6
  datetime_format: "iso8601"
  column_validation_ignore_suffixes:
    - "_quality_report.csv"
    - "_correlation_report.csv"
    - "_qc.csv"
    - "_metadata.csv"
    - "_qc_summary.json"
    - "qc_missing_mappings.csv"
    - "qc_enrichment_metrics.csv"
    - "_summary_statistics.csv"
    - "_dataset_metrics.csv"
```

## 5) Соответствие схемам и проверка на этапе validate()

Все таблицы MUST соответствовать своим Pandera-схемам: типы, обязательность, диапазоны, категориальные множества, а также межколоночные проверки (например, согласованность standard_value/unit). Примеры паттернов и классов см. в официальной документации Pandera.

Ошибки валидации являются блокирующими; частичный вывод запрещён. Это предотвращает распространение невалидных данных.

## 6) Гарантии детерминизма при записи

Функция write() MUST обеспечивать: фиксированный порядок колонок, стабильную сортировку по бизнес-ключам, одинаковую сериализацию и атомарную замену целевого файла.

Для кроссплатформенной атомарности применяется библиотека atomicwrites, которая пишет во временный файл на той же ФС и затем выполняет атомарную замену (на POSIX — rename/replace).

## 7) Сопутствующие ссылки на спецификации API

Crossref REST API и «Tips/Etiquette».

OpenAlex Works и поля/селект/лимиты.

NCBI E-utilities: обзор и параметры.

Semantic Scholar: поля fields.

UniProt REST и списки полей.

PubChem PUG REST/PUG-View.

ChEMBL web services и интерактивные доки.

IUPHAR/BPS GtoP web services.

Pydantic BaseModel (`src/bioetl/config/models.py`).

Pandera DataFrameSchema/Checks.

Python hashlib SHA256 (`src/bioetl/core/hashing.py`) и `determinism.hash_algorithm` (`src/bioetl/configs/includes/determinism.yaml`).

[^determinism-hash]: Конфигурация алгоритма детерминизма хранится в [src/bioetl/configs/includes/determinism.yaml](../src/bioetl/configs/includes/determinism.yaml) и устанавливает значение `sha256` для поля `hash_algorithm`.

