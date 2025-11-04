# IUPHAR Target Extraction Pipeline

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

This document describes the `target-iuphar` pipeline, which is responsible for extracting and processing target data from the Guide to Pharmacology (GtP) / IUPHAR database.

## 1. Overview

The `target-iuphar` pipeline extracts information about pharmacological targets from the Guide to Pharmacology API. This data is essential for understanding target classifications, pharmacological properties, and receptor families. The pipeline supports flexible input formats, allowing identification of targets through various identifiers including IUPHAR target_id, UniProt accession, target name, gene name, or other IDs.

**Note:** This pipeline supports multiple input formats and automatically resolves identifiers to IUPHAR target_id through the search API. Enrichment from external sources (UniProt, ChEMBL) is handled by separate pipelines.

## 2. CLI Command

The pipeline is executed via the `target-iuphar` CLI command.

**Usage:**

```bash
python -m bioetl.cli.main target-iuphar [OPTIONS]
```

**Example:**

```bash
python -m bioetl.cli.main target-iuphar \
  --config configs/pipelines/iuphar/target.yaml \
  --output-dir data/output/target-iuphar
```

## 3. Configuration

### 3.1 Обзор конфигурации

Target-IUPHAR pipeline управляется через декларативный YAML-файл конфигурации. Все конфигурационные файлы валидируются во время выполнения против строго типизированных Pydantic-моделей, что гарантирует корректность параметров перед запуском пайплайна.

**Расположение конфига:** `configs/pipelines/iuphar/target.yaml`

**Профили по умолчанию:** Конфигурация наследует от `configs/profiles/base.yaml` и `configs/profiles/determinism.yaml` через `extends`.

**Основной источник:** Guide to Pharmacology API `https://www.guidetopharmacology.org/DATA`.

### 3.2 Структура конфигурации

Конфигурационный файл Target-IUPHAR pipeline следует стандартной структуре `PipelineConfig`:

```yaml
# configs/pipelines/iuphar/target.yaml

version: 1  # Версия схемы конфигурации

extends:  # Профили для наследования
  - ../profiles/base.yaml
  - ../profiles/determinism.yaml

# -----------------------------------------------------------------------------
# Метаданные пайплайна
# -----------------------------------------------------------------------------
pipeline:
  name: "target-iuphar"
  version: "1.0.0"
  owner: "iuphar-team"
  description: "Extract and normalize IUPHAR target data"

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
      max_calls: 6  # Конфигурируемо, по умолчанию 6 req/sec
      period: 1.0
    rate_limit_jitter: true
    headers:
      User-Agent: "BioETL/1.0 (UnifiedAPIClient)"
      Accept: "application/json"
      x-api-key: "${IUPHAR_API_KEY}"  # Опционально, если предоставлен
  
  # Именованный профиль для IUPHAR
  profiles:
    iuphar:
      timeout_sec: 30.0
      retries:
        total: 7
      rate_limit:
        max_calls: 6
        period: 1.0
      headers:
        x-api-key: "${IUPHAR_API_KEY}"  # Опционально

# -----------------------------------------------------------------------------
# Кэширование
# -----------------------------------------------------------------------------
cache:
  enabled: true
  directory: "http_cache"
  ttl: 3600  # 1 час (кэш короче, чем для ChEMBL/UniProt)
  namespace: "iuphar"

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
  iuphar:
    enabled: true
    description: "Guide to Pharmacology API"
    http_profile: "iuphar"  # Ссылка на именованный HTTP-профиль
    base_url: "https://www.guidetopharmacology.org/DATA"
    parameters:
      endpoint: "/targets"
      families_endpoint: "/targets/families"
      search_endpoint: "/targets/search"
    pagination:
      enabled: true
      page_size: 100  # Размер страницы для пагинации
      max_pages: null  # Без ограничения (null)

# -----------------------------------------------------------------------------
# Обогащение
# -----------------------------------------------------------------------------
enrichments:
  iuphar:
    min: 0.6  # Минимальный порог успешности обогащения (60%)

# -----------------------------------------------------------------------------
# Детерминизм
# -----------------------------------------------------------------------------
determinism:
  enabled: true
  hash_policy_version: "1.0.0"
  float_precision: 6
  datetime_format: "iso8601"
  column_validation_ignore_suffixes: ["_scd", "_temp", "_meta", "_tmp"]
  
  # Ключи сортировки (обязательно: первый ключ - iuphar_target_id)
  sort:
    by: ["iuphar_target_id"]
    ascending: [true]
    na_position: "last"
  
  # Фиксированный порядок колонок (из IUPHARTargetSchema.Config.column_order)
  column_order:
    - "iuphar_target_id"
    - "iuphar_family_id"
    - "target_name"
    - "gene_name"
    - "uniprot_accession"
    - "iuphar_type"
    - "iuphar_class"
    - "iuphar_subclass"
    # ... остальные колонки в порядке из IUPHARTargetSchema.Config.column_order
  
  # Хеширование
  hashing:
    algorithm: "sha256"
    row_fields: []  # Все колонки из column_order (кроме exclude_fields)
    business_key_fields: ["iuphar_target_id"]
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
  schema_in: "bioetl.schemas.iuphar.target.IUPHARTargetInputSchema"  # Опционально
  schema_out: "bioetl.schemas.iuphar.target.IUPHARTargetOutputSchema"  # Обязательно
  strict: true  # Строгая проверка порядка колонок
  coerce: true  # Приведение типов в Pandera

# -----------------------------------------------------------------------------
# Материализация
# -----------------------------------------------------------------------------
materialization:
  root: "data/output"
  default_format: "parquet"
  pipeline_subdir: "target-iuphar"
  filename_template: "target_iuphar_{date_tag}.{format}"

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
| `http.profiles.iuphar.rate_limit.max_calls` | `6` | Рекомендуемый лимит для IUPHAR API (конфигурируемо) | `<= 10` (предупреждение при превышении) |
| `determinism.sort.by[0]` | `"iuphar_target_id"` | Первый ключ сортировки должен быть бизнес-ключом | Обязательно |
| `determinism.column_order` | Полный список колонок | Полный список колонок из `IUPHARTargetSchema.Config.column_order` | Проверяется на соответствие схеме |
| `validation.schema_out` | `"bioetl.schemas.iuphar.target.IUPHARTargetOutputSchema"` | Обязательная ссылка на Pandera-схему | Должен существовать и быть импортируемым |
| `enrichments.iuphar.min` | `0.6` | Минимальный порог успешности обогащения | `>= 0.0 and <= 1.0` |

### 3.4 Валидация конфигурации

Конфигурация валидируется через Pydantic-модель `PipelineConfig` при загрузке:

1. **Типобезопасность:** Все значения проверяются на соответствие типам
2. **Обязательные поля:** Отсутствие обязательных полей приводит к ошибке
3. **Неизвестные ключи:** Неизвестные ключи запрещены (`extra="forbid"`)
4. **Кросс-полевые инварианты:** Проверка согласованности (например, длина `sort.by` и `sort.ascending`)

### 3.5 Переопределения через CLI

Параметры конфигурации могут быть переопределены через CLI флаг `--set`:

```bash
python -m bioetl.cli.main target-iuphar \
  --config configs/pipelines/iuphar/target.yaml \
  --output-dir data/output/target-iuphar \
  --set sources.iuphar.pagination.page_size=50 \
  --set enrichments.iuphar.min=0.7 \
  --set determinism.sort.by='["iuphar_target_id"]'
```

### 3.6 Переменные окружения

Наивысший приоритет имеют переменные окружения (формат: `BIOETL__<SECTION>__<KEY>__<SUBKEY>`):

```bash
export IUPHAR_API_KEY="your_api_key_here"  # Опционально, для аутентификации
export BIOETL__SOURCES__IUPHAR__PAGINATION__PAGE_SIZE=100
export BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=90
export BIOETL__ENRICHMENTS__IUPHAR__MIN=0.6
```

### 3.7 CLI режимы

CLI поддерживает следующие режимы выполнения:

- **`default`**: Стандартный режим выполнения (по умолчанию)
- **`smoke`**: Режим для быстрой проверки (ограниченное количество записей)

**Пример использования режима:**

```bash
python -m bioetl.cli.main target-iuphar \
  --config configs/pipelines/iuphar/target.yaml \
  --output-dir data/output/target-iuphar \
  --mode smoke
```

### 3.8 Пример полного конфига

Полный пример конфигурационного файла для target-iuphar pipeline доступен в `configs/pipelines/iuphar/target.yaml`. Конфигурация включает все необходимые секции для работы пайплайна с детерминизмом, валидацией и извлечением данных из IUPHAR.

For detailed configuration structure and API, see [Typed Configurations and Profiles](docs/configs/00-typed-configs-and-profiles.md).

## 4. Data Schemas

### 4.1 Обзор

Target-IUPHAR pipeline использует Pandera для строгой валидации данных перед записью. Схема валидации определяет структуру, типы данных, порядок колонок и ограничения для всех записей. Подробности о политике Pandera схем см. в [Pandera Schema Policy](docs/schemas/00-pandera-policy.md).

**Расположение схемы:** `src/bioetl/schemas/iuphar/target/iuphar_target_output_schema.py`

**Ссылка в конфиге:** `validation.schema_out: "bioetl.schemas.iuphar.target.IUPHARTargetOutputSchema"`

**Версионирование:** Схема имеет семантическую версию (`MAJOR.MINOR.PATCH`), которая фиксируется в `meta.yaml` для каждой записи пайплайна.

### 4.2 Требования к схеме

Схема валидации для target-iuphar pipeline должна соответствовать следующим требованиям:

1. **Строгость:** `strict=True` - все колонки должны быть явно определены
2. **Приведение типов:** `coerce=True` - автоматическое приведение типов данных
3. **Порядок колонок:** `ordered=True` - фиксированный порядок колонок
4. **Nullable dtypes:** Использование nullable dtypes (`pd.StringDtype()`, `pd.Int64Dtype()`, `pd.Float64Dtype()`) вместо `object`
5. **Бизнес-ключ:** Валидация уникальности `iuphar_target_id`

### 4.3 Структура схемы

Ниже приведена структура Pandera схемы для target-iuphar pipeline:

```python
# src/bioetl/schemas/iuphar/target/iuphar_target_output_schema.py

import pandera as pa
from pandera.typing import Series, DateTime, String, Int64, Float64
from typing import Optional

# Версия схемы
SCHEMA_VERSION = "1.0.0"

class IUPHARTargetOutputSchema(pa.DataFrameModel):
    """Pandera schema for IUPHAR target output data."""
    
    # Бизнес-ключ (обязательное поле, NOT NULL)
    iuphar_target_id: Series[Int64] = pa.Field(
        description="IUPHAR target identifier",
        nullable=False
    )
    
    # Основные поля IUPHAR target
    iuphar_family_id: Series[Int64] = pa.Field(
        description="IUPHAR family identifier",
        nullable=True
    )
    target_name: Series[str] = pa.Field(
        description="Target name",
        nullable=True
    )
    gene_name: Series[str] = pa.Field(
        description="Gene name",
        nullable=True
    )
    uniprot_accession: Series[str] = pa.Field(
        description="UniProt accession (if mapped)",
        nullable=True
    )
    
    # Классификация
    iuphar_type: Series[str] = pa.Field(
        description="IUPHAR target type",
        nullable=True
    )
    iuphar_class: Series[str] = pa.Field(
        description="IUPHAR target class",
        nullable=True
    )
    iuphar_subclass: Series[str] = pa.Field(
        description="IUPHAR target subclass",
        nullable=True
    )
    
    # Дополнительные поля
    target_description: Series[str] = pa.Field(
        description="Target description",
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
        description="Source system (IUPHAR or IUPHAR_FALLBACK)",
        nullable=False,
        isin=["IUPHAR", "IUPHAR_FALLBACK"]
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
            "iuphar_target_id",
            "iuphar_family_id",
            "target_name",
            "gene_name",
            "uniprot_accession",
            "iuphar_type",
            "iuphar_class",
            "iuphar_subclass",
            "target_description",
            "organism",
            "organism_id",
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
    @pa.check("iuphar_target_id")
    def check_unique_target_id(cls, series: Series[Int64]) -> Series[bool]:
        """Validate uniqueness of iuphar_target_id."""
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

1. **Хранение:** Golden CSV/Parquet и `meta.yaml` находятся в `tests/golden/target-iuphar/`
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

- **CSV файл:** CSV с одной или несколькими колонками идентификаторов
- **DataFrame:** Pandas DataFrame с колонками идентификаторов

**Гибкий формат входных данных:**

Пайплайн поддерживает множественные форматы входных данных. Может быть предоставлена одна или несколько из следующих колонок:

- `iuphar_target_id` (Int64, NOT NULL, если известен): IUPHAR target identifier
- `uniprot_accession` (StringDtype): UniProt accession для поиска через search API
- `target_name` (StringDtype): Название мишени для поиска
- `gene_name` (StringDtype): Имя гена для поиска
- `id` (StringDtype): Другой идентификатор для поиска

**Приоритет разрешения идентификаторов:**

1. Если предоставлен `iuphar_target_id`, он используется напрямую
2. Если предоставлен `uniprot_accession`, выполняется поиск через `/targets/search`
3. Если предоставлен `target_name`, выполняется поиск через `/targets/search`
4. Если предоставлен `gene_name`, выполняется поиск через `/targets/search`
5. Если предоставлен `id`, выполняется поиск через `/targets/search`

**Схема валидации входных данных:**

```python
# src/bioetl/schemas/iuphar/target/iuphar_target_input_schema.py

class IUPHARTargetInputSchema(pa.DataFrameModel):
    """Flexible input schema for IUPHAR target pipeline."""
    
    # Хотя бы одно поле должно быть заполнено
    iuphar_target_id: Series[Int64] = pa.Field(
        description="IUPHAR target identifier",
        nullable=True
    )
    uniprot_accession: Series[str] = pa.Field(
        description="UniProt accession for search",
        nullable=True
    )
    target_name: Series[str] = pa.Field(
        description="Target name for search",
        nullable=True
    )
    gene_name: Series[str] = pa.Field(
        description="Gene name for search",
        nullable=True
    )
    id: Series[str] = pa.Field(
        description="Other identifier for search",
        nullable=True
    )

    class Config:
        strict = True
        coerce = True
    
    @pa.check("iuphar_target_id", "uniprot_accession", "target_name", "gene_name", "id")
    def check_at_least_one_identifier(cls, df: pd.DataFrame) -> Series[bool]:
        """Validate that at least one identifier is provided."""
        has_id = (
            df["iuphar_target_id"].notna() |
            df["uniprot_accession"].notna() |
            df["target_name"].notna() |
            df["gene_name"].notna() |
            df["id"].notna()
        )
        return has_id
```

**Примеры входных CSV:**

**Пример 1: С iuphar_target_id**
```csv
iuphar_target_id
1234
5678
9012
```

**Пример 2: С uniprot_accession**
```csv
uniprot_accession
P12345
P67890
```

**Пример 3: С target_name**
```csv
target_name
Dopamine D2 receptor
Serotonin 5-HT1A receptor
```

**Пример 4: Смешанный формат**
```csv
iuphar_target_id,uniprot_accession,target_name
1234,,
,,"Dopamine D2 receptor"
,,P12345
```

### 5.2 Выходные данные

**Структура выходного CSV/Parquet:**

Выходной файл содержит все поля из `IUPHARTargetOutputSchema` в фиксированном порядке колонок, определенном в схеме.

**Обязательные артефакты:**

- `target_iuphar_{date_tag}.csv` или `target_iuphar_{date_tag}.parquet` — основной датасет с данными IUPHAR target
- `target_iuphar_{date_tag}_quality_report.csv` — QC метрики и отчет о качестве данных

**Опциональные артефакты (extended режим):**

- `target_iuphar_{date_tag}_meta.yaml` — метаданные запуска пайплайна
- `target_iuphar_{date_tag}_run_manifest.json` — манифест запуска (опционально)

**Формат имен файлов:**

- Дата-тег: `YYYYMMDD` (например, `20250115`)
- Формат: определяется параметром `materialization.default_format` (по умолчанию `parquet`)
- Пример: `target_iuphar_20250115.parquet`, `target_iuphar_20250115_quality_report.csv`

**Структура выходных данных:**

Выходной файл содержит следующие группы полей:

1. **Бизнес-ключ:** `iuphar_target_id`
2. **Основные поля IUPHAR target:** `iuphar_family_id`, `target_name`, `gene_name`, `uniprot_accession`
3. **Классификация:** `iuphar_type`, `iuphar_class`, `iuphar_subclass`
4. **Дополнительные поля:** `target_description`, `organism`, `organism_id`
5. **Системные метаданные:** `run_id`, `git_commit`, `config_hash`, `pipeline_version`, `source_system`, `extracted_at`
6. **Хеши:** `hash_row`, `hash_business_key`
7. **Индекс:** `index`

**Пример структуры выходного файла:**

```csv
iuphar_target_id,iuphar_family_id,target_name,gene_name,uniprot_accession,iuphar_type,iuphar_class,iuphar_subclass,...,run_id,git_commit,config_hash,pipeline_version,source_system,extracted_at,hash_row,hash_business_key,index
1234,56,Dopamine D2 receptor,DRD2,P14416,Receptor,GPCR,Class A Rhodopsin-like,...,a1b2c3d4e5f6g7h8,abc123...,def456...,1.0.0,IUPHAR,2025-01-15T10:30:00Z,abc123...,def456...,0
```

## 6. Component Architecture

The `target-iuphar` pipeline follows the standard source architecture, utilizing a stack of specialized components for its operation. Pipeline focuses on extracting data from Guide to Pharmacology API with support for pagination and flexible identifier resolution.

| Component | Implementation |
|---|---|
| **Client** | `src/bioetl/sources/iuphar/client/iuphar_client.py` — HTTP client for Guide to Pharmacology API |
| **Parser** | `src/bioetl/sources/iuphar/parser/iuphar_parser.py` — парсер для обработки ответов API |
| **Normalizer** | `src/bioetl/sources/iuphar/normalizer/iuphar_normalizer.py` — нормализация данных IUPHAR |
| **Paginator** | `src/bioetl/sources/iuphar/pagination/page_number_paginator.py` — пагинация для `/targets` и `/targets/families` |
| **SearchResolver** | `src/bioetl/sources/iuphar/resolver/search_resolver.py` — разрешение различных идентификаторов в IUPHAR target_id |
| **Service** | `src/bioetl/sources/iuphar/service.py` — `IupharService` с `IupharServiceConfig` для сопоставления идентификаторов |
| **Schema** | `src/bioetl/schemas/iuphar/target/iuphar_target_output_schema.py` — Pandera schema для валидации |

**Public API:**

- `bioetl.sources.iuphar.pipeline.GtpIupharPipeline` — полнофункциональный ETL для Guide to Pharmacology
- `bioetl.sources.iuphar.service.IupharService` (`IupharServiceConfig`) — сервис для сопоставления IUPHAR-идентификаторов

**Module layout:**

- Источник содержит выделенные слои: HTTP клиент (`client/IupharClient`), билдеры запросов и пагинацию (`request.py`, `pagination/PageNumberPaginator`), парсер, normalizer и schema

**Tests:**

- `tests/unit/test_iuphar_pipeline.py` — проверка экстракции, материализации и интеграции пагинатора
- `tests/sources/iuphar/test_client.py` — HTTP client tests
- `tests/sources/iuphar/test_parser.py` — parser tests
- `tests/sources/iuphar/test_normalizer.py` — normalizer tests
- `tests/sources/iuphar/test_schema.py` — schema tests
- `tests/sources/iuphar/test_pipeline_e2e.py` — end-to-end tests

## 7. Key Identifiers

- **Business Key**: `iuphar_target_id` — уникальный идентификатор IUPHAR target
- **Sort Key**: `iuphar_target_id` — используется для детерминированной сортировки перед записью
- **Secondary Key**: `iuphar_family_id` — идентификатор семейства таргетов

## 8. Детерминизм

**Sort keys:** `["iuphar_target_id"]`

Target-IUPHAR pipeline обеспечивает детерминированный вывод через стабильную сортировку и хеширование:

- **Sort keys:** Строки сортируются по `iuphar_target_id` перед записью
- **Hash policy:** Используется SHA256 для генерации `hash_row` и `hash_business_key`
  - `hash_row`: хеш всей строки (кроме полей `generated_at`, `run_id`)
  - `hash_business_key`: хеш бизнес-ключа (`iuphar_target_id`)
- **Canonicalization:** Все значения нормализуются перед хешированием (trim whitespace, lowercase identifiers, fixed precision numbers, UTC timestamps)
- **Column order:** Фиксированный порядок колонок из Pandera схемы
- **Meta.yaml:** Содержит `pipeline_version`, `row_count`, checksums, `hash_algo`, `hash_policy_version`

**Guarantees:**

- Бит-в-бит воспроизводимость при одинаковых входных данных и конфигурации
- Стабильный порядок строк и колонок
- Идентичные хеши для идентичных данных

For detailed policy, see [Determinism Policy](docs/determinism/01-determinism-policy.md).

## 9. QC/QA

**Ключевые метрики успеха:**

| Метрика | Target | Критичность |
|---------|--------|-------------|
| **IUPHAR coverage** | 100% идентификаторов | HIGH |
| **Search resolution success rate** | ≥90% для поисковых запросов | HIGH |
| **Classification completeness** | ≥85% для targets с классификацией | MEDIUM |
| **Family coverage** | ≥80% для targets с family_id | MEDIUM |
| **Pipeline failure rate** | 0% (graceful degradation) | CRITICAL |
| **Детерминизм** | Бит-в-бит воспроизводимость | CRITICAL |

**QC метрики:**

- Покрытие IUPHAR: процент успешно извлеченных iuphar_target_id
- Успешность поиска: процент успешных разрешений идентификаторов через search API
- Полнота классификации: процент targets с полной информацией о типе/классе/подклассе
- Покрытие семейств: процент targets с iuphar_family_id
- Валидность данных: соответствие схеме Pandera и референциальная целостность

**Пороги качества:**

- IUPHAR coverage должен быть 100% (критично)
- Search resolution success rate ≥90% для поисковых запросов (высокий приоритет)
- Classification completeness ≥85% (средний приоритет)
- Family coverage ≥80% (средний приоритет)

**QC отчеты:**

- Генерируется `target_iuphar_quality_report.csv` с метриками покрытия и валидности
- При использовании `--extended` режима дополнительно создается подробный отчет с распределениями

**Merge policy:**

Матрица источников фиксирует, что таргеты агрегируют ChEMBL (основа), UniProt (имена/гены) и IUPHAR (классификация); приоритеты: UniProt > ChEMBL для маркерных полей, IUPHAR > ChEMBL для классов.

For detailed QC metrics and policies, see [QC Overview](docs/qc/00-qc-overview.md).

## 10. Логирование и трассировка

Target-IUPHAR pipeline использует `UnifiedLogger` для структурированного логирования всех операций с обязательными полями контекста.

**Обязательные поля в логах:**

- `run_id`: Уникальный идентификатор запуска пайплайна
- `stage`: Текущая стадия выполнения (`extract`, `transform`, `validate`, `write`)
- `pipeline`: Имя пайплайна (`target-iuphar`)
- `duration`: Время выполнения стадии в секундах
- `row_count`: Количество обработанных строк

**Структурированные события:**

- `pipeline_started`: Начало выполнения пайплайна
- `extract_started`: Начало стадии извлечения
- `extract_completed`: Завершение стадии извлечения с метриками
- `search_resolution_started`: Начало разрешения идентификаторов через search
- `search_resolution_completed`: Завершение разрешения идентификаторов
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
  "pipeline": "target-iuphar",
  "timestamp": "2025-01-15T10:30:00.123456Z"
}

{
  "event": "search_resolution_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "target-iuphar",
  "duration": 12.5,
  "total_identifiers": 500,
  "resolved_count": 485,
  "unresolved_count": 15,
  "resolution_success_rate": 97.0,
  "timestamp": "2025-01-15T10:30:12.678901Z"
}

{
  "event": "extract_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "target-iuphar",
  "duration": 45.2,
  "row_count": 1250,
  "classification_completeness": 87.5,
  "family_coverage": 82.3,
  "timestamp": "2025-01-15T10:30:45.345678Z"
}

{
  "event": "pipeline_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "bootstrap",
  "pipeline": "target-iuphar",
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

For detailed logging configuration and API, see [Logging Overview](docs/logging/00-overview.md).

