# Ввод и вывод: договоры, схемы и конфиг

## 1) Ввод (Input Contract)

Источник истины ввода — официальные REST-интерфейсы провайдеров. Извлечение выполняется только через client/ с учётом лимитов, ретраев и этикета API.

`extract()` каждого пайплайна возвращает табличное представление данных — строго `pd.DataFrame` с именованными колонками. Тип контракта закреплён в базовом классе пайплайна, поэтому все реализации наследников (`document`, `activity`, `target`, `testitem`, …) выдают именно таблицу, даже если промежуточно работают со словарями/JSON.【F:src/bioetl/pipelines/base.py†L847-L877】

Полученный датафрейм сразу связывается со схемой из централизованного `schema_registry`. Реестр фиксирует `schema_id`, `schema_version`, `column_order`, `na_policy` и `precision_policy`, что затем попадает в метаданные и используется для fail-fast проверки дрейфа колонок.【F:src/bioetl/schemas/registry.py†L22-L109】【F:tests/unit/test_output_writer.py†L520-L571】

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

Контроль целостности: хеши строк и наборов бизнес-ключей (например, BLAKE2b) фиксируются в метаданых экспорта; алгоритм и размер дайджеста стабильны.

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

```yaml
# meta.yaml

run_id: "abc123"
pipeline_version: "2.1.0"
config_hash: "sha256:deadbeef..."
config_snapshot:
  path: "src/bioetl/configs/pipelines/document.yaml"
  sha256: "sha256:d1c2..."
chembl_release: "33"
row_count: 12345
column_count: 42
column_order:
  - "document_chembl_id"
  - "title"
  # ... все колонки

checksums:
  dataset: "sha256:abc123..."
  quality: "sha256:def456..."
  correlation: "sha256:ghi789..."  # Опционально, только если correlation enabled
git_commit: "a1b2c3d"
generated_at: "2025-01-28T14:23:15.123Z"
lineage:
  source_files:
    - "input/documents.csv"
  transformations:
    - "normalize_titles"
    - "validate_dois"
```

Фактическая реализация `UnifiedOutputWriter._write_metadata()` добавляет в файл ключи `file_checksums` и `artifacts` (dataset, quality_report, дополнительные наборы, QC-артефакты), копирует `schema_id`, `schema_version`, `column_order_source`, `na_policy`, `precision_policy` из registry, прикладывает `config_snapshot` (если доступен) и опциональные блоки `qc_summary`, `qc_metrics`, `validation_issues`, `runtime_options`. Даже при отсутствии пользовательской lineage функция создаёт структуру `source_files`/`transformations`, сохраняя run_id, pipeline_version, config_hash, git_commit, список sources и отметку времени генерации.【F:src/bioetl/core/output_writer.py†L962-L1058】

**Обязательные поля lineage конфигурации:**

- `run_id`: уникальный идентификатор запуска (UUID8 или timestamp-based)
- `config_hash`: SHA256 хеш конфигурации (после резолва переменных окружения)
- `config_snapshot`: путь и хеш исходного файла конфигурации

**Обоснование:** Обеспечивает полную воспроизводимость и аудит запусков.

## 3) Схемы данных (UnifiedSchema)

**Валидация выполняется Pandera-схемами из Schema Registry.**

### Schema Registry

Централизованный реестр Pandera-схем с версионированием.

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

Конфиг источника лежит в:
```
src/bioetl/configs/pipelines/<source>.yaml
```

Допускаются include-модули (например, `includes/chembl_source.yaml`) для вынесения общих блоков и сокращения дублирования между
пайплайнами. После подстановки всех include-файлов итоговый YAML автоматически валидируется объектом `PipelineConfig`; ошибки
схемы или несовместимые ключи MUST прерывать запуск.

### 4.1 JSON Schema для базового конфига

Для машинной проверки структура описывается JSON Schema (Draft 2020-12).

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "SourceConfig",
  "type": "object",
  "required": ["api_base_url", "http", "pagination", "fields", "output", "logging"],
  "properties": {
    "api_base_url": { "type": "string", "format": "uri" },
    "auth": {
      "type": "object",
      "properties": {
        "api_key": { "type": ["string", "null"] },
        "token": { "type": ["string", "null"] }
      },
      "additionalProperties": false
    },
    "http": {
      "type": "object",
      "required": ["timeout_s", "retries", "backoff", "rate_limit_rps", "headers"],
      "properties": {
        "timeout_s": { "type": "number", "minimum": 1 },
        "retries":   { "type": "integer", "minimum": 0 },
        "backoff":   { "type": "object", "properties": {
            "strategy": { "type": "string", "enum": ["exponential", "jittered"] },
            "base_s":   { "type": "number", "minimum": 0.1 },
            "max_s":    { "type": "number", "minimum": 0.1 }
          }, "required": ["strategy","base_s","max_s"], "additionalProperties": false },
        "rate_limit_rps": { "type": "number", "minimum": 0.1 },
        "headers": { "type": "object", "additionalProperties": { "type": "string" } }
      },
      "additionalProperties": false
    },
    "pagination": {
      "type": "object",
      "required": ["type"],
      "properties": {
        "type": { "type": "string", "enum": ["page", "cursor", "offset_limit", "token"] },
        "page_size": { "type": ["integer","null"], "minimum": 1 },
        "cursor_param": { "type": ["string","null"] },
        "max_pages": { "type": ["integer","null"], "minimum": 1 }
      },
      "additionalProperties": false
    },
    "filters": { "type": "object", "additionalProperties": true },
    "fields":  { "type": "array", "items": { "type": "string" } },
    "output": {
      "type": "object",
      "required": ["path", "format", "column_order", "hashing"],
      "properties": {
        "path": { "type": "string" },
        "format": { "type": "string", "enum": ["csv","parquet"] },
        "column_order": { "type": "array", "items": { "type": "string" } },
        "hashing": {
          "type": "object",
          "required": ["algo","digest_size"],
          "properties": {
            "algo": { "type": "string", "enum": ["blake2b","blake2s"] },
            "digest_size": { "type": "integer", "minimum": 8, "maximum": 64 }
          },
          "additionalProperties": false
        }
      },
      "additionalProperties": false
    },
    "logging": {
      "type": "object",
      "required": ["level"],
      "properties": {
        "level": { "type": "string", "enum": ["DEBUG","INFO","WARNING","ERROR"] }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

### 4.2 YAML-пример конфига (Crossref)

```yaml
api_base_url: "https://api.crossref.org/works"
auth: { api_key: null }
http:
  timeout_s: 30
  retries: 3
  backoff: { strategy: exponential, base_s: 1.0, max_s: 10.0 }
  rate_limit_rps: 5
  headers:
    User-Agent: "bioetl/0.13 (+https://example.org)"
    mailto: "[email protected]"    # соответствует этикету Crossref/OpenAlex
pagination:
  type: cursor
  page_size: 200
  cursor_param: "cursor"
  max_pages: 500
filters:
  from-pub-date: "2018-01-01"
  type: "journal-article"
fields: ["DOI","title","author","container-title","issued","link","publisher"]
output:
  path: "data/crossref/"
  format: "csv"
  column_order: ["document_id","doi","title","venue","year","authors","urls","source","ingest_timestamp"]
  hashing: { algo: blake2b, digest_size: 32 }
logging:
  level: "INFO"
```

**Комментарии:**

headers.mailto обязателен для «политного» доступа в Crossref/OpenAlex.

pagination.type: cursor согласуется с рекомендуемым режимом OpenAlex и курсорными интерфейсами, где применимо.

### 4.3 Пример для PubMed (E-utilities)

```yaml
api_base_url: "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
http:
  timeout_s: 60
  retries: 5
  backoff: { strategy: exponential, base_s: 1.0, max_s: 30.0 }
  rate_limit_rps: 3
  headers:
    User-Agent: "bioetl/0.13 (+https://example.org)"
pagination:
  type: page
  page_size: 10000      # retmax
  max_pages: 100
filters:
  db: "pubmed"
  term: "histamine[tiab] AND 2020:2025[dp]"
  rettype: "medline"
  retmode: "xml"
fields: ["pmid","title","abstract","authors","journal","year"]
output:
  path: "data/pubmed/"
  format: "csv"
  column_order: ["document_id","pmid","title","venue","year","authors","abstract","source","ingest_timestamp"]
  hashing: { algo: blake2b, digest_size: 32 }
logging:
  level: "INFO"
```

Семантика ESearch/EFetch и параметров retmode/rettype соответствует руководству NCBI.

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

JSON Schema Draft 2020-12.

Pandera DataFrameSchema/Checks.

Python hashlib BLAKE2.

