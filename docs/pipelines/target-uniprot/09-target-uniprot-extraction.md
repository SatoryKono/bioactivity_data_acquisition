# UniProt Target Extraction Pipeline

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

This document describes the `target-uniprot` pipeline, which is responsible for extracting and processing target data from the UniProt database.

## 1. Overview

The `target-uniprot` pipeline extracts information about protein targets from the UniProt REST API. This data is essential for understanding protein structure, function, and interactions. The pipeline focuses on extracting comprehensive protein information including sequences, features, gene names, and organism data.

**Note:** This pipeline requires UniProt accession numbers as input. For mapping ChEMBL target IDs to UniProt accessions, use the separate `chembl2uniprot-mapping` pipeline.

## 2. CLI Command

The pipeline is executed via the `target-uniprot` CLI command.

**Usage:**

```bash
python -m bioetl.cli.app target-uniprot [OPTIONS]
```

**Example:**

```bash
python -m bioetl.cli.app target-uniprot \
  --config configs/pipelines/uniprot/target.yaml \
  --output-dir data/output/target-uniprot
```

## 3. Configuration

### 3.1 Обзор конфигурации

Target-UniProt pipeline управляется через декларативный YAML-файл конфигурации. Все конфигурационные файлы валидируются во время выполнения против строго типизированных Pydantic-моделей, что гарантирует корректность параметров перед запуском пайплайна.

**Расположение конфига:** `configs/pipelines/uniprot/target.yaml`

**Профили по умолчанию:** Конфигурация наследует от `configs/defaults/base.yaml` и `configs/defaults/determinism.yaml` через `extends`.

**Основной источник:** UniProt REST API `https://rest.uniprot.org`.

### 3.2 Структура конфигурации

Конфигурационный файл Target-UniProt pipeline следует стандартной структуре `PipelineConfig`:

```yaml
# configs/pipelines/uniprot/target.yaml

version: 1  # Версия схемы конфигурации

extends:  # Профили для наследования
  - ../profiles/base.yaml
  - ../profiles/determinism.yaml

# -----------------------------------------------------------------------------
# Метаданные пайплайна
# -----------------------------------------------------------------------------
pipeline:
  name: "target-uniprot"
  version: "1.0.0"
  owner: "uniprot-team"
  description: "Extract and normalize UniProt target data"

# -----------------------------------------------------------------------------
# HTTP-конфигурация
# -----------------------------------------------------------------------------
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
      max_calls: 2  # КРИТИЧЕСКИ: <= 2 (квота UniProt API)
      period: 1.0
    rate_limit_jitter: true
    headers:
      User-Agent: "BioETL/1.0 (UnifiedAPIClient)"
      Accept: "application/json"

  # Именованный профиль для UniProt
  profiles:
    uniprot:
      timeout_sec: 30.0
      retries:
        total: 7
      rate_limit:
        max_calls: 2
        period: 1.0

# -----------------------------------------------------------------------------
# Кэширование
# -----------------------------------------------------------------------------
cache:
  enabled: true
  directory: "http_cache"
  ttl: 86400  # 24 часа
  namespace: "uniprot"  # Обеспечивает namespace-scoped invalidation

# -----------------------------------------------------------------------------
# Пути
# -----------------------------------------------------------------------------
paths:
  input_root: "data/input"
  output_root: "data/output"
  cache_root: ".cache"

# -----------------------------------------------------------------------------
# Источники данных
# -----------------------------------------------------------------------------
sources:
  uniprot:
    enabled: true
    description: "UniProt REST API"
    http_profile: "uniprot"  # Ссылка на именованный HTTP-профиль
    batch_size: 100  # Размер батча для batch requests
    base_url: "https://rest.uniprot.org"
    parameters:
      endpoint: "/uniprotkb"
      format: "json"  # json, tsv, xml (конфигурируемо)

  # ID Mapping (опционально, для обогащения)
  uniprot_idmapping:
    enabled: false  # Отключен для основного пайплайна
    base_url: "https://rest.uniprot.org"

  # Ortholog lookups (опционально)
  uniprot_orthologs:
    enabled: true  # Включен для поиска ортологов
    base_url: "https://rest.uniprot.org"
    parameters:
      endpoint: "/uniprotkb/{accession}/orthologs"

# -----------------------------------------------------------------------------
# Детерминизм
# -----------------------------------------------------------------------------
determinism:
  enabled: true
  hash_policy_version: "1.0.0"
  float_precision: 6
  datetime_format: "iso8601"
  column_validation_ignore_suffixes: ["_scd", "_temp", "_meta", "_tmp"]

  # Ключи сортировки (обязательно: первый ключ - uniprot_accession)
  sort:
    by: ["uniprot_accession"]
    ascending: [true]
    na_position: "last"

  # Фиксированный порядок колонок (из UniProtTargetSchema.Config.column_order)
  column_order:
    - "uniprot_accession"
    - "entry_name"
    - "protein_name"
    - "gene_names"
    - "organism"
    - "organism_id"
    - "sequence"
    - "sequence_length"
    # ... остальные колонки в порядке из UniProtTargetSchema.Config.column_order

  # Хеширование
  hashing:
    algorithm: "sha256"
    row_fields: []  # Все колонки из column_order (кроме exclude_fields)
    business_key_fields: ["uniprot_accession"]
    exclude_fields: ["generated_at", "run_id"]

  # Сериализация
  serialization:
    csv:
      separator: ","
      quoting: "ALL"
      na_rep: ""
    booleans: ["True", "False"]
    nan_rep: "NaN"

  # Окружение
  environment:
    timezone: "UTC"
    locale: "C"

  # Запись
  write:
    strategy: "atomic"

  # Метаданные
  meta:
    location: "sibling"
    include_fields: []
    exclude_fields: []

# -----------------------------------------------------------------------------
# Валидация
# -----------------------------------------------------------------------------
validation:
  schema_in: "bioetl.schemas.uniprot.target.UniProtTargetInputSchema"  # Опционально
  schema_out: "bioetl.schemas.uniprot.target.UniProtTargetOutputSchema"  # Обязательно
  strict: true  # Строгая проверка порядка колонок
  coerce: true  # Приведение типов в Pandera

# -----------------------------------------------------------------------------
# Материализация
# -----------------------------------------------------------------------------
materialization:
  root: "data/output"
  default_format: "parquet"
  pipeline_subdir: "target-uniprot"
  filename_template: "target_uniprot_{date_tag}.{format}"

# -----------------------------------------------------------------------------
# Fallback механизмы
# -----------------------------------------------------------------------------
fallbacks:
  enabled: true
  max_depth: null  # Без ограничения глубины
```

### 3.3 Критические параметры

| Параметр | Значение | Обоснование | Валидация |
|----------|----------|------------|-----------|
| `http.profiles.uniprot.rate_limit.max_calls` | `2` | Жёсткое ограничение квоты UniProt API (2 req/sec) | `if max_calls > 2: raise ConfigValidationError` |
| `determinism.sort.by[0]` | `"uniprot_accession"` | Первый ключ сортировки должен быть бизнес-ключом | Обязательно |
| `determinism.column_order` | Полный список колонок | Полный список колонок из `UniProtTargetSchema.Config.column_order` | Проверяется на соответствие схеме |
| `validation.schema_out` | `"bioetl.schemas.uniprot.target.UniProtTargetOutputSchema"` | Обязательная ссылка на Pandera-схему | Должен существовать и быть импортируемым |
| `sources.uniprot_orthologs.enabled` | `true` | Включение поиска ортологов | Опционально |

### 3.4 Валидация конфигурации

Конфигурация валидируется через Pydantic-модель `PipelineConfig` при загрузке:

1. **Типобезопасность:** Все значения проверяются на соответствие типам
2. **Обязательные поля:** Отсутствие обязательных полей приводит к ошибке
3. **Неизвестные ключи:** Неизвестные ключи запрещены (`extra="forbid"`)
4. **Кросс-полевые инварианты:** Проверка согласованности (например, длина `sort.by` и `sort.ascending`)

**Пример ошибки валидации:**

```text
1 validation error for PipelineConfig
http.profiles.uniprot.rate_limit.max_calls
  Value error, rate_limit.max_calls must be <= 2 due to UniProt API quota limit
```

### 3.5 Переопределения через CLI

Параметры конфигурации могут быть переопределены через CLI флаг `--set`:

```bash
python -m bioetl.cli.app target-uniprot \
  --config configs/pipelines/uniprot/target.yaml \
  --output-dir data/output/target-uniprot \
  --set sources.uniprot.batch_size=50 \
  --set sources.uniprot_orthologs.enabled=false \
  --set determinism.sort.by='["uniprot_accession"]'
```

### 3.6 Переменные окружения

Наивысший приоритет имеют переменные окружения (формат: `BIOETL__<SECTION>__<KEY>__<SUBKEY>`):

```bash
export UNIPROT_API_KEY="your_api_key_here"  # Опционально, передается в UnifiedAPIClient
export BIOETL__SOURCES__UNIPROT__BATCH_SIZE=100
export BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=90
export BIOETL__DETERMINISM__FLOAT_PRECISION=4
```

### 3.7 Пример полного конфига

Полный пример конфигурационного файла для target-uniprot pipeline доступен в `configs/pipelines/uniprot/target.yaml`. Конфигурация включает все необходимые секции для работы пайплайна с детерминизмом, валидацией и извлечением данных из UniProt.

For detailed configuration structure and API, see [Typed Configurations and Profiles](../configs/00-typed-configs-and-profiles.md).

## 4. Data Schemas

### 4.1 Обзор

Target-UniProt pipeline использует Pandera для строгой валидации данных перед записью. Схема валидации определяет структуру, типы данных, порядок колонок и ограничения для всех записей. Подробности о политике Pandera схем см. в [Pandera Schema Policy](../schemas/00-pandera-policy.md).

**Расположение схемы:** `src/bioetl/schemas/uniprot/target/uniprot_target_output_schema.py`

**Ссылка в конфиге:** `validation.schema_out: "bioetl.schemas.uniprot.target.UniProtTargetOutputSchema"`

**Версионирование:** Схема имеет семантическую версию (`MAJOR.MINOR.PATCH`), которая фиксируется в `meta.yaml` для каждой записи пайплайна.

### 4.2 Требования к схеме

Схема валидации для target-uniprot pipeline должна соответствовать следующим требованиям:

1. **Строгость:** `strict=True` - все колонки должны быть явно определены
2. **Приведение типов:** `coerce=True` - автоматическое приведение типов данных
3. **Порядок колонок:** `ordered=True` - фиксированный порядок колонок
4. **Nullable dtypes:** Использование nullable dtypes (`pd.StringDtype()`, `pd.Int64Dtype()`, `pd.Float64Dtype()`) вместо `object`
5. **Бизнес-ключ:** Валидация уникальности `uniprot_accession`

### 4.3 Структура схемы

Ниже приведена структура Pandera схемы для target-uniprot pipeline:

```python
# src/bioetl/schemas/uniprot/target/uniprot_target_output_schema.py

import pandera as pa
from pandera.typing import Series, DateTime, String, Int64, Float64
from typing import Optional

# Версия схемы
SCHEMA_VERSION = "1.0.0"

class UniProtTargetOutputSchema(pa.DataFrameModel):
    """Pandera schema for UniProt target output data."""

    # Бизнес-ключ (обязательное поле, NOT NULL)
    uniprot_accession: Series[str] = pa.Field(
        description="UniProt accession identifier",
        nullable=False,
        regex="^[A-NR-Z][0-9]([A-Z][A-Z, 0-9][A-Z, 0-9][0-9]){1,2}$|^[OPQ][0-9][A-Z0-9]{3}[0-9]$"
    )

    # Основные поля UniProt entry
    entry_name: Series[str] = pa.Field(
        description="UniProt entry name",
        nullable=True
    )
    protein_name: Series[str] = pa.Field(
        description="Full protein name",
        nullable=True
    )
    gene_names: Series[str] = pa.Field(
        description="Gene names (comma-separated)",
        nullable=True
    )
    organism: Series[str] = pa.Field(
        description="Organism name",
        nullable=True
    )
    organism_id: Series[Int64] = pa.Field(
        description="NCBI taxonomy ID",
        nullable=True
    )

    # Sequence data
    sequence: Series[str] = pa.Field(
        description="Protein sequence",
        nullable=True
    )
    sequence_length: Series[Int64] = pa.Field(
        description="Sequence length",
        nullable=True
    )

    # Features and annotations
    features: Series[str] = pa.Field(
        description="Features (JSON string)",
        nullable=True
    )

    # Orthologs (если включено)
    ortholog_count: Series[Int64] = pa.Field(
        description="Number of orthologs",
        nullable=True
    )

    # Isoform information
    isoform_count: Series[Int64] = pa.Field(
        description="Number of isoforms",
        nullable=True
    )

    # Системные метаданные
    run_id: Series[str] = pa.Field(
        description="Pipeline run ID",
        nullable=False
    )
    git_commit: Series[str] = pa.Field(
        description="Git commit SHA",
        nullable=False
    )
    config_hash: Series[str] = pa.Field(
        description="Configuration hash",
        nullable=False
    )
    pipeline_version: Series[str] = pa.Field(
        description="Pipeline version",
        nullable=False
    )
    source_system: Series[str] = pa.Field(
        description="Source system (UniProt or UniProt_FALLBACK)",
        nullable=False,
        isin=["UniProt", "UniProt_FALLBACK"]
    )
    extracted_at: Series[DateTime] = pa.Field(
        description="Extraction timestamp (UTC)",
        nullable=False
    )

    # Хеши
    hash_row: Series[str] = pa.Field(
        description="SHA256 hash of entire row",
        nullable=False,
        regex="^[a-f0-9]{64}$"
    )
    hash_business_key: Series[str] = pa.Field(
        description="SHA256 hash of business key",
        nullable=False,
        regex="^[a-f0-9]{64}$"
    )

    # Индекс
    index: Series[Int64] = pa.Field(
        description="Row index",
        nullable=False
    )

    # Порядок колонок
    class Config:
        strict = True
        coerce = True
        ordered = True
        column_order = [
            "uniprot_accession",
            "entry_name",
            "protein_name",
            "gene_names",
            "organism",
            "organism_id",
            "sequence",
            "sequence_length",
            "features",
            "ortholog_count",
            "isoform_count",
            # ... остальные колонки в фиксированном порядке
            "run_id",
            "git_commit",
            "config_hash",
            "pipeline_version",
            "source_system",
            "extracted_at",
            "hash_row",
            "hash_business_key",
            "index"
        ]

    # Валидация уникальности бизнес-ключа
    @pa.check("uniprot_accession")
    def check_unique_accession(cls, series: Series[str]) -> Series[bool]:
        """Validate uniqueness of uniprot_accession."""
        return ~series.duplicated()
```

### 4.4 Версионирование схемы

Схема версионируется по семантическому версионированию (`MAJOR.MINOR.PATCH`):

- **PATCH:** Обновления документации или корректировки, не влияющие на логику валидации
- **MINOR:** Обратно совместимые расширения (добавление nullable колонок с дефолтами, ослабление ограничений)
- **MAJOR:** Breaking changes (переименование/удаление колонок, изменение типов, изменение порядка колонок)

**Инвариант:** Версия схемы фиксируется в `meta.yaml` для каждой записи пайплайна:

```yaml
schema_version: "1.0.0"
```

### 4.5 Процесс валидации

Валидация выполняется в стадии `validate` пайплайна (`PipelineBase.validate()`):

1. **Загрузка схемы:** Динамическая загрузка схемы из `validation.schema_out`
2. **Lazy validation:** Выполнение `schema.validate(df, lazy=True)` для сбора всех ошибок
3. **Проверка порядка колонок:** Применение `ensure_column_order()` для соответствия `column_order`
4. **Запись версии:** Фиксация `schema_version` в `meta.yaml`

**Режимы валидации:**

- **Fail-closed (по умолчанию):** Пайплайн завершается при первой ошибке валидации
- **Fail-open (опционально):** Ошибки логируются как предупреждения, `schema_valid: false` в `meta.yaml`

### 4.6 Golden-тесты

Golden-артефакты обеспечивают регрессионное покрытие для поведения схемы:

1. **Хранение:** Golden CSV/Parquet и `meta.yaml` находятся в `tests/bioetl/golden/target-uniprot/`
2. **Триггеры регенерации:**
   - Изменение версии схемы (любой уровень)
   - Изменение политики детерминизма
   - Обновление правил сортировки или хеширования
3. **Процесс:**
   - Запуск пайплайна с `--golden` для получения свежих артефактов
   - Выполнение тестов схемы
   - Проверка хешей и порядка колонок
   - Коммит обновленных golden-файлов вместе с изменениями версии схемы

## 5. Inputs and Outputs

### 5.1 Входные данные

**Формат входных данных:**

Входные данные могут быть предоставлены в следующих форматах:

- **CSV файл:** CSV с колонкой `uniprot_accession`
- **DataFrame:** Pandas DataFrame с колонкой `uniprot_accession`

**Обязательные поля:**

- `uniprot_accession` (StringDtype, NOT NULL): UniProt accession identifier в формате `P12345` или `A0A0A0M1X5` (регулярное выражение для валидации формата)

**Опциональные поля:**

- Поля для фильтрации по organism или organism_id (если поддерживается)

**Схема валидации входных данных:**

```python
# src/bioetl/schemas/uniprot/target/uniprot_target_input_schema.py

class UniProtTargetInputSchema(pa.DataFrameModel):
    uniprot_accession: Series[str] = pa.Field(
        description="UniProt accession identifier",
        nullable=False,
        regex="^[A-NR-Z][0-9]([A-Z][A-Z, 0-9][A-Z, 0-9][0-9]){1,2}$|^[OPQ][0-9][A-Z0-9]{3}[0-9]$"
    )

    class Config:
        strict = True
        coerce = True
```

**Пример входного CSV:**

```csv
uniprot_accession
P12345
P67890
A0A0A0M1X5
```

### 5.2 Выходные данные

**Структура выходного CSV/Parquet:**

Выходной файл содержит все поля из `UniProtTargetOutputSchema` в фиксированном порядке колонок, определенном в схеме.

**Обязательные артефакты:**

- `target_uniprot_{date_tag}.csv` или `target_uniprot_{date_tag}.parquet` — основной датасет с данными UniProt target
- `target_uniprot_{date_tag}_quality_report.csv` — QC метрики и отчет о качестве данных

**Опциональные артефакты (extended режим):**

- `target_uniprot_{date_tag}_meta.yaml` — метаданные запуска пайплайна
- `target_uniprot_{date_tag}_run_manifest.json` — манифест запуска (опционально)

**Формат имен файлов:**

- Дата-тег: `YYYYMMDD` (например, `20250115`)
- Формат: определяется параметром `materialization.default_format` (по умолчанию `parquet`)
- Пример: `target_uniprot_20250115.parquet`, `target_uniprot_20250115_quality_report.csv`

**Структура выходных данных:**

Выходной файл содержит следующие группы полей:

1. **Бизнес-ключ:** `uniprot_accession`
2. **Основные поля UniProt entry:** `entry_name`, `protein_name`, `gene_names`, `organism`, `organism_id`, `sequence`, `sequence_length`
3. **Features и annotations:** `features` (JSON string)
4. **Orthologs:** `ortholog_count` (если включено через `sources.uniprot_orthologs.enabled`)
5. **Isoforms:** `isoform_count`
6. **Системные метаданные:** `run_id`, `git_commit`, `config_hash`, `pipeline_version`, `source_system`, `extracted_at`
7. **Хеши:** `hash_row`, `hash_business_key`
8. **Индекс:** `index`

**Пример структуры выходного файла:**

```csv
uniprot_accession,entry_name,protein_name,gene_names,organism,organism_id,sequence,sequence_length,...,run_id,git_commit,config_hash,pipeline_version,source_system,extracted_at,hash_row,hash_business_key,index
P12345,INS_HUMAN,Insulin,INS,9606,Homo sapiens,9606,MALWMRLLPLL...,110,...,a1b2c3d4e5f6g7h8,abc123...,def456...,1.0.0,UniProt,2025-01-15T10:30:00Z,abc123...,def456...,0
```

## 6. Component Architecture

The `target-uniprot` pipeline follows the standard source architecture, utilizing a stack of specialized components for its operation. Pipeline focuses on extracting data from UniProt REST API.

| Component | Implementation |
|---|---|
| **Client** | `src/bioetl/sources/uniprot/client/uniprot_client.py` — HTTP client for UniProt REST API |
| **Parser** | `src/bioetl/sources/uniprot/parser/uniprot_parser.py` — parsing helpers and isoform expansion |
| **Normalizer** | `src/bioetl/sources/uniprot/normalizer/uniprot_normalizer.py` — dataframe normalisation and enrichment fallbacks |
| **Service** | `src/bioetl/sources/uniprot/service.py` — enrichment orchestration (client calls, parsing, normalisation helpers) |
| **Schema** | `src/bioetl/schemas/uniprot/target/uniprot_target_output_schema.py` — Pandera schema for validation |

**Public API:**

- `from bioetl.integrations.uniprot import UniProtService` *(планируется к внедрению)*
- `from bioetl.integrations.uniprot import UniProtEnrichmentResult` *(планируется к внедрению)*
- `from bioetl.pipelines.uniprot import UniProtPipeline`

**Module layout:**

- `src/bioetl/sources/uniprot/service.py` — enrichment orchestration (client calls, parsing, normalisation helpers)
- `src/bioetl/pipelines/uniprot.py` — standalone CLI pipeline wrapper
- `src/bioetl/sources/uniprot/pipeline.py` — compatibility proxy for the source registry

**Tests:**

- `tests/bioetl/sources/uniprot/test_client.py` — HTTP client adapters (`fetch_entries`, ID mapping, ortholog lookups)
- `tests/bioetl/sources/uniprot/test_parser.py` — parsing helpers and isoform expansion
- `tests/bioetl/sources/uniprot/test_normalizer.py` — dataframe normalisation and enrichment fallbacks
- `tests/bioetl/sources/uniprot/test_pipeline_e2e.py` — pipeline orchestration happy path

## 7. Key Identifiers

- **Business Key**: `uniprot_accession` — уникальный идентификатор UniProt entry
- **Sort Key**: `uniprot_accession` — используется для детерминированной сортировки перед записью

## 8. Детерминизм

**Sort keys:** `["uniprot_accession"]`

Target-UniProt pipeline обеспечивает детерминированный вывод через стабильную сортировку и хеширование:

- **Sort keys:** Строки сортируются по `uniprot_accession` перед записью
- **Hash policy:** Используется SHA256 для генерации `hash_row` и `hash_business_key`
  - `hash_row`: хеш всей строки (кроме полей `generated_at`, `run_id`)
  - `hash_business_key`: хеш бизнес-ключа (`uniprot_accession`)
- **Canonicalization:** Все значения нормализуются перед хешированием (trim whitespace, lowercase identifiers, fixed precision numbers, UTC timestamps)
- **Column order:** Фиксированный порядок колонок из Pandera схемы
- **Meta.yaml:** Содержит `pipeline_version`, `row_count`, checksums, `hash_algo`, `hash_policy_version`

**Guarantees:**

- Бит-в-бит воспроизводимость при одинаковых входных данных и конфигурации
- Стабильный порядок строк и колонок
- Идентичные хеши для идентичных данных

For detailed policy, see [Determinism Policy](../determinism/00-determinism-policy.md).

## 9. QC/QA

**Ключевые метрики успеха:**

| Метрика | Target | Критичность |
|---------|--------|-------------|
| **UniProt coverage** | 100% идентификаторов | HIGH |
| **Ortholog coverage** | ≥80% для proteins с orthologs | MEDIUM |
| **Isoform completeness** | ≥90% для proteins с isoforms | MEDIUM |
| **Sequence completeness** | ≥95% для protein entries | HIGH |
| **Pipeline failure rate** | 0% (graceful degradation) | CRITICAL |
| **Детерминизм** | Бит-в-бит воспроизводимость | CRITICAL |

**QC метрики:**

- Покрытие UniProt: процент успешно извлеченных uniprot_accession
- Покрытие ортологов: процент proteins с успешным обогащением через ortholog lookups
- Полнота изоформ: процент proteins с полной информацией об изоформах
- Полнота последовательностей: процент entries с полной sequence информацией
- Валидность данных: соответствие схеме Pandera и референциальная целостность

**Пороги качества:**

- UniProt coverage должен быть 100% (критично)
- Ortholog coverage ≥80% для proteins с orthologs (средний приоритет)
- Isoform completeness ≥90% для proteins с isoforms (средний приоритет)
- Sequence completeness ≥95% (высокий приоритет)

**QC отчеты:**

- Генерируется `target_uniprot_quality_report.csv` с метриками покрытия и валидности
- При использовании `--extended` режима дополнительно создается подробный отчет с распределениями

For detailed QC metrics and policies, see [QC Overview](../qc/00-qc-overview.md).

## 10. Логирование и трассировка

Target-UniProt pipeline использует `UnifiedLogger` для структурированного логирования всех операций с обязательными полями контекста.

**Обязательные поля в логах:**

- `run_id`: Уникальный идентификатор запуска пайплайна
- `stage`: Текущая стадия выполнения (`extract`, `transform`, `validate`, `write`)
- `pipeline`: Имя пайплайна (`target-uniprot`)
- `duration`: Время выполнения стадии в секундах
- `row_count`: Количество обработанных строк

**Структурированные события:**

- `pipeline_started`: Начало выполнения пайплайна
- `extract_started`: Начало стадии извлечения
- `extract_completed`: Завершение стадии извлечения с метриками
- `transform_started`: Начало стадии трансформации
- `transform_completed`: Завершение стадии трансформации
- `validate_started`: Начало валидации
- `validate_completed`: Завершение валидации
- `write_started`: Начало записи результатов
- `write_completed`: Завершение записи результатов
- `pipeline_completed`: Успешное завершение пайплайна
- `pipeline_failed`: Ошибка выполнения с деталями

**Примеры JSON-логов:**

```json
{
  "event": "pipeline_started",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "bootstrap",
  "pipeline": "target-uniprot",
  "timestamp": "2025-01-15T10:30:00.123456Z"
}

{
  "event": "extract_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "target-uniprot",
  "duration": 45.2,
  "row_count": 1250,
  "ortholog_coverage": 82.5,
  "isoform_completeness": 92.3,
  "timestamp": "2025-01-15T10:30:45.345678Z"
}

{
  "event": "pipeline_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "bootstrap",
  "pipeline": "target-uniprot",
  "duration": 120.5,
  "row_count": 1250,
  "timestamp": "2025-01-15T10:32:00.678901Z"
}
```

**Формат вывода:**

- Консоль: текстовый формат для удобства чтения
- Файлы: JSON формат для машинной обработки и анализа
- Ротация: автоматическая ротация лог-файлов (10MB × 10 файлов)

**Трассировка:**

- Все операции связаны через `run_id` для отслеживания полного жизненного цикла пайплайна
- Каждая стадия логирует начало и завершение с метриками производительности
- Ошибки логируются с полным контекстом и stack trace

For detailed logging configuration and API, see [Logging Overview](../logging/00-overview.md).
