# ChEMBL to UniProt Mapping Pipeline

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

This document describes the `chembl2uniprot-mapping` pipeline, which is responsible for mapping ChEMBL target identifiers to UniProt accession numbers through the UniProt ID Mapping API.

## 1. Overview

The `chembl2uniprot-mapping` pipeline creates mappings between ChEMBL `target_chembl_id` and UniProt accession numbers. This mapping is essential for enriching ChEMBL target data with UniProt protein information. The pipeline uses the UniProt ID Mapping API, which provides an asynchronous mapping service that can handle large volumes of identifiers.

**Note:** This pipeline is separate from the target extraction pipelines. It focuses solely on creating the mapping table between ChEMBL and UniProt identifiers. The resulting mappings can then be used by other pipelines to enrich target data.

## 2. CLI Command

The pipeline is executed via the `chembl2uniprot-mapping` CLI command.

**Usage:**

```bash
# (not implemented)
python -m bioetl.cli.app chembl2uniprot-mapping [OPTIONS]
```

**Example:**

```bash
# (not implemented)
python -m bioetl.cli.app chembl2uniprot-mapping \
  --config configs/pipelines/uniprot/chembl2uniprot.yaml \
  --output-dir data/output/chembl2uniprot-mapping
```

## 3. Configuration

### 3.1 Обзор конфигурации

ChEMBL2UniProt Mapping pipeline управляется через декларативный YAML-файл конфигурации. Все конфигурационные файлы валидируются во время выполнения против строго типизированных Pydantic-моделей, что гарантирует корректность параметров перед запуском пайплайна.

**Расположение конфига:** `configs/pipelines/uniprot/chembl2uniprot.yaml`

**Профили по умолчанию:** Конфигурация наследует от `configs/defaults/base.yaml` и `configs/defaults/determinism.yaml` через `extends`.

**Основной источник:** UniProt ID Mapping API `https://rest.uniprot.org/idmapping`.

### 3.2 Структура конфигурации

Конфигурационный файл ChEMBL2UniProt Mapping pipeline следует стандартной структуре `PipelineConfig`:

```yaml
# configs/pipelines/uniprot/chembl2uniprot.yaml

version: 1  # Версия схемы конфигурации

extends:  # Профили для наследования
  - ../profiles/base.yaml
  - ../profiles/determinism.yaml

# -----------------------------------------------------------------------------
# Метаданные пайплайна
# -----------------------------------------------------------------------------
pipeline:
  name: "chembl2uniprot-mapping"
  version: "1.0.0"
  owner: "uniprot-team"
  description: "Map ChEMBL target IDs to UniProt accessions"

# -----------------------------------------------------------------------------
# HTTP-конфигурация
# -----------------------------------------------------------------------------
http:
  default:
    timeout_sec: 120.0  # Увеличенный таймаут для async операций
    connect_timeout_sec: 15.0
    read_timeout_sec: 120.0
    retries:
      total: 5
      backoff_multiplier: 2.0
      backoff_max: 120.0
      statuses: [408, 429, 500, 502, 503, 504]
    rate_limit:
      max_calls: 2  # КРИТИЧЕСКИ: <= 2 (квота UniProt API)
      period: 1.0
    rate_limit_jitter: true
    headers:
      User-Agent: "BioETL/1.0 (UnifiedAPIClient)"
      Accept: "application/json"

  # Именованный профиль для UniProt ID Mapping
  profiles:
    uniprot_idmapping:
      timeout_sec: 180.0  # Увеличенный таймаут для polling статуса
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
  namespace: "uniprot_idmapping"  # Обеспечивает namespace-scoped invalidation

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
  uniprot_idmapping:
    enabled: true
    description: "UniProt ID Mapping API"
    http_profile: "uniprot_idmapping"  # Ссылка на именованный HTTP-профиль
    batch_size: 100000  # Размер батча для ID mapping (может быть большим)
    base_url: "https://rest.uniprot.org"
    parameters:
      from: "CHEMBL_ID"  # Источник: ChEMBL ID
      to: "UniProtKB"    # Назначение: UniProt KB
      endpoint_run: "/idmapping/run"
      endpoint_status: "/idmapping/status"
      endpoint_stream: "/idmapping/stream"
    polling:
      enabled: true
      interval_sec: 5.0  # Интервал опроса статуса (в секундах)
      max_polling_attempts: 60  # Максимальное количество попыток опроса
      timeout_sec: 300  # Таймаут для завершения job (5 минут)

# -----------------------------------------------------------------------------
# Детерминизм
# -----------------------------------------------------------------------------
determinism:
  enabled: true
  hash_policy_version: "1.0.0"
  float_precision: 6
  datetime_format: "iso8601"
  column_validation_ignore_suffixes: ["_scd", "_temp", "_meta", "_tmp"]

  # Ключи сортировки (обязательно: первый ключ - target_chembl_id, затем uniprot_accession)
  sort:
    by: ["target_chembl_id", "uniprot_accession"]
    ascending: [true, true]
    na_position: "last"

  # Фиксированный порядок колонок (из ChEMBL2UniProtMappingSchema.Config.column_order)
  column_order:
    - "target_chembl_id"
    - "uniprot_accession"
    - "confidence_score"
    - "mapping_status"
    - "mapping_source"
    # ... остальные колонки в порядке из ChEMBL2UniProtMappingSchema.Config.column_order

  # Хеширование
  hashing:
    algorithm: "sha256"
    row_fields: []  # Все колонки из column_order (кроме exclude_fields)
    business_key_fields: ["target_chembl_id", "uniprot_accession"]  # Составной ключ
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
  schema_in: "bioetl.schemas.uniprot.mapping.ChEMBL2UniProtMappingInputSchema"  # Опционально
  schema_out: "bioetl.schemas.uniprot.mapping.ChEMBL2UniProtMappingOutputSchema"  # Обязательно
  strict: true  # Строгая проверка порядка колонок
  coerce: true  # Приведение типов в Pandera

# -----------------------------------------------------------------------------
# Материализация
# -----------------------------------------------------------------------------
materialization:
  root: "data/output"
  default_format: "parquet"
  pipeline_subdir: "chembl2uniprot-mapping"
  filename_template: "chembl2uniprot_mapping_{date_tag}.{format}"

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
| `http.profiles.uniprot_idmapping.rate_limit.max_calls` | `2` | Жёсткое ограничение квоты UniProt API (2 req/sec) | `if max_calls > 2: raise ConfigValidationError` |
| `sources.uniprot_idmapping.parameters.from` | `"CHEMBL_ID"` | Источник идентификаторов (ChEMBL ID) | Обязательно |
| `sources.uniprot_idmapping.parameters.to` | `"UniProtKB"` | Назначение (UniProt KB) | Обязательно |
| `sources.uniprot_idmapping.polling.interval_sec` | `5.0` | Интервал опроса статуса job | `>= 1.0` |
| `sources.uniprot_idmapping.polling.max_polling_attempts` | `60` | Максимальное количество попыток опроса | `>= 1` |
| `determinism.sort.by[0]` | `"target_chembl_id"` | Первый ключ сортировки | Обязательно |
| `determinism.hashing.business_key_fields` | `["target_chembl_id", "uniprot_accession"]` | Составной бизнес-ключ | Обязательно |
| `validation.schema_out` | `"bioetl.schemas.uniprot.mapping.ChEMBL2UniProtMappingOutputSchema"` | Обязательная ссылка на Pandera-схему | Должен существовать и быть импортируемым |

### 3.4 Валидация конфигурации

Конфигурация валидируется через Pydantic-модель `PipelineConfig` при загрузке:

1. **Типобезопасность:** Все значения проверяются на соответствие типам
2. **Обязательные поля:** Отсутствие обязательных полей приводит к ошибке
3. **Неизвестные ключи:** Неизвестные ключи запрещены (`extra="forbid"`)
4. **Кросс-полевые инварианты:** Проверка согласованности (например, длина `sort.by` и `sort.ascending`)

**Пример ошибки валидации:**

```text
1 validation error for PipelineConfig
http.profiles.uniprot_idmapping.rate_limit.max_calls
  Value error, rate_limit.max_calls must be <= 2 due to UniProt API quota limit
```

### 3.5 Переопределения через CLI

Параметры конфигурации могут быть переопределены через CLI флаг `--set`:

```bash
# (not implemented)
python -m bioetl.cli.app chembl2uniprot-mapping \
  --config configs/pipelines/uniprot/chembl2uniprot.yaml \
  --output-dir data/output/chembl2uniprot-mapping \
  --set sources.uniprot_idmapping.polling.interval_sec=10.0 \
  --set sources.uniprot_idmapping.polling.max_polling_attempts=120
```

### 3.6 Переменные окружения

Наивысший приоритет имеют переменные окружения (формат: `BIOETL__<SECTION>__<KEY>__<SUBKEY>`):

```bash
export UNIPROT_API_KEY="your_api_key_here"  # Опционально, передается в UnifiedAPIClient
export BIOETL__SOURCES__UNIPROT_IDMAPPING__BATCH_SIZE=100000
export BIOETL__SOURCES__UNIPROT_IDMAPPING__POLLING__INTERVAL_SEC=5.0
export BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=180
```

### 3.7 Пример полного конфига

Полный пример конфигурационного файла для chembl2uniprot-mapping pipeline доступен в `configs/pipelines/uniprot/chembl2uniprot.yaml`. Конфигурация включает все необходимые секции для работы пайплайна с детерминизмом, валидацией и асинхронным маппингом через UniProt ID Mapping API.

For detailed configuration structure and API, see [Typed Configurations and Profiles](../configs/00-typed-configs-and-profiles.md).

## 4. Data Schemas

### 4.1 Обзор

ChEMBL2UniProt Mapping pipeline использует Pandera для строгой валидации данных перед записью. Схема валидации определяет структуру, типы данных, порядок колонок и ограничения для всех записей. Подробности о политике Pandera схем см. в [Pandera Schema Policy](../schemas/00-pandera-policy.md).

**Расположение схемы:** `src/bioetl/schemas/uniprot/mapping/chembl2uniprot_mapping_output_schema.py`

**Ссылка в конфиге:** `validation.schema_out: "bioetl.schemas.uniprot.mapping.ChEMBL2UniProtMappingOutputSchema"`

**Версионирование:** Схема имеет семантическую версию (`MAJOR.MINOR.PATCH`), которая фиксируется в `meta.yaml` для каждой записи пайплайна.

### 4.2 Требования к схеме

Схема валидации для chembl2uniprot-mapping pipeline должна соответствовать следующим требованиям:

1. **Строгость:** `strict=True` - все колонки должны быть явно определены
2. **Приведение типов:** `coerce=True` - автоматическое приведение типов данных
3. **Порядок колонок:** `ordered=True` - фиксированный порядок колонок
4. **Nullable dtypes:** Использование nullable dtypes (`pd.StringDtype()`, `pd.Int64Dtype()`, `pd.Float64Dtype()`) вместо `object`
5. **Составной бизнес-ключ:** Валидация уникальности комбинации `(target_chembl_id, uniprot_accession)`

### 4.3 Структура схемы

Ниже приведена структура Pandera схемы для chembl2uniprot-mapping pipeline:

```python
# src/bioetl/schemas/uniprot/mapping/chembl2uniprot_mapping_output_schema.py

import pandera as pa
from pandera.typing import Series, DateTime, String, Int64, Float64
from typing import Optional

# Версия схемы
SCHEMA_VERSION = "1.0.0"

class ChEMBL2UniProtMappingOutputSchema(pa.DataFrameModel):
    """Pandera schema for ChEMBL to UniProt mapping output data."""

    # Составной бизнес-ключ (обязательные поля, NOT NULL)
    target_chembl_id: Series[str] = pa.Field(
        description="ChEMBL target identifier",
        nullable=False,
        regex="^CHEMBL\\d+$"
    )
    uniprot_accession: Series[str] = pa.Field(
        description="UniProt accession identifier",
        nullable=False,
        regex="^[A-NR-Z][0-9]([A-Z][A-Z, 0-9][A-Z, 0-9][0-9]){1,2}$|^[OPQ][0-9][A-Z0-9]{3}[0-9]$"
    )

    # Маппинг метаданные
    confidence_score: Series[Float64] = pa.Field(
        description="Confidence score for the mapping (0.0-1.0)",
        nullable=True,
        ge=0.0,
        le=1.0
    )
    mapping_status: Series[str] = pa.Field(
        description="Status of the mapping",
        nullable=False,
        isin=["success", "partial", "failed", "ambiguous"]
    )
    mapping_source: Series[str] = pa.Field(
        description="Source of the mapping",
        nullable=False,
        isin=["UniProt_IDMapping", "UniProt_IDMapping_FALLBACK"]
    )

    # One-to-many detection
    is_primary_mapping: Series[pa.typing.Bool] = pa.Field(
        description="Whether this is the primary mapping (for one-to-many cases)",
        nullable=True
    )
    total_mappings_count: Series[Int64] = pa.Field(
        description="Total number of mappings for this target_chembl_id",
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
        description="Source system (UniProt_IDMapping or UniProt_IDMapping_FALLBACK)",
        nullable=False,
        isin=["UniProt_IDMapping", "UniProt_IDMapping_FALLBACK"]
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
        description="SHA256 hash of composite business key",
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
            "target_chembl_id",
            "uniprot_accession",
            "confidence_score",
            "mapping_status",
            "mapping_source",
            "is_primary_mapping",
            "total_mappings_count",
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

    # Валидация уникальности составного бизнес-ключа
    @pa.check("target_chembl_id", "uniprot_accession")
    def check_unique_composite_key(cls, df: pd.DataFrame) -> Series[bool]:
        """Validate uniqueness of composite key (target_chembl_id, uniprot_accession)."""
        return ~df[["target_chembl_id", "uniprot_accession"]].duplicated()
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

1. **Хранение:** Golden CSV/Parquet и `meta.yaml` находятся в `tests/bioetl/golden/chembl2uniprot-mapping/`
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

- **CSV файл:** CSV с колонкой `target_chembl_id`
- **DataFrame:** Pandas DataFrame с колонкой `target_chembl_id`

**Обязательные поля:**

- `target_chembl_id` (StringDtype, NOT NULL): ChEMBL идентификатор target в формате `CHEMBL\d+`

**Опциональные поля:**

- Поля для фильтрации или дополнительной информации (не используются в маппинге)

**Схема валидации входных данных:**

```python
# src/bioetl/schemas/uniprot/mapping/chembl2uniprot_mapping_input_schema.py

class ChEMBL2UniProtMappingInputSchema(pa.DataFrameModel):
    target_chembl_id: Series[str] = pa.Field(
        description="ChEMBL target identifier",
        nullable=False,
        regex="^CHEMBL\\d+$"
    )

    class Config:
        strict = True
        coerce = True
```

**Пример входного CSV:**

```csv
target_chembl_id
CHEMBL240
CHEMBL231
CHEMBL232
```

### 5.2 Выходные данные

**Структура выходного CSV/Parquet:**

Выходной файл содержит все поля из `ChEMBL2UniProtMappingOutputSchema` в фиксированном порядке колонок, определенном в схеме.

**Обязательные артефакты:**

- `chembl2uniprot_mapping_{date_tag}.csv` или `chembl2uniprot_mapping_{date_tag}.parquet` — основной датасет с маппингами
- `chembl2uniprot_mapping_{date_tag}_quality_report.csv` — QC метрики и отчет о качестве данных

**Опциональные артефакты (extended режим):**

- `chembl2uniprot_mapping_{date_tag}_meta.yaml` — метаданные запуска пайплайна
- `chembl2uniprot_mapping_{date_tag}_run_manifest.json` — манифест запуска (опционально)

**Формат имен файлов:**

- Дата-тег: `YYYYMMDD` (например, `20250115`)
- Формат: определяется параметром `materialization.default_format` (по умолчанию `parquet`)
- Пример: `chembl2uniprot_mapping_20250115.parquet`, `chembl2uniprot_mapping_20250115_quality_report.csv`

**Структура выходных данных:**

Выходной файл содержит следующие группы полей:

1. **Составной бизнес-ключ:** `target_chembl_id`, `uniprot_accession`
2. **Маппинг метаданные:** `confidence_score`, `mapping_status`, `mapping_source`
3. **One-to-many detection:** `is_primary_mapping`, `total_mappings_count`
4. **Системные метаданные:** `run_id`, `git_commit`, `config_hash`, `pipeline_version`, `source_system`, `extracted_at`
5. **Хеши:** `hash_row`, `hash_business_key`
6. **Индекс:** `index`

**Важно:** Один `target_chembl_id` может маппиться на несколько `uniprot_accession` (изоформы). Каждая комбинация `(target_chembl_id, uniprot_accession)` является отдельной записью в выходном файле.

**Пример структуры выходного файла:**

```csv
target_chembl_id,uniprot_accession,confidence_score,mapping_status,mapping_source,is_primary_mapping,total_mappings_count,...,run_id,git_commit,config_hash,pipeline_version,source_system,extracted_at,hash_row,hash_business_key,index
CHEMBL240,P12345,0.95,success,UniProt_IDMapping,true,1,...,a1b2c3d4e5f6g7h8,abc123...,def456...,1.0.0,UniProt_IDMapping,2025-01-15T10:30:00Z,abc123...,def456...,0
CHEMBL231,P67890,0.92,success,UniProt_IDMapping,true,2,...,a1b2c3d4e5f6g7h8,abc123...,def456...,1.0.0,UniProt_IDMapping,2025-01-15T10:30:00Z,abc123...,def456...,1
CHEMBL231,P67891,0.88,success,UniProt_IDMapping,false,2,...,a1b2c3d4e5f6g7h8,abc123...,def456...,1.0.0,UniProt_IDMapping,2025-01-15T10:30:00Z,abc123...,def456...,2
```

## 6. Component Architecture

The `chembl2uniprot-mapping` pipeline follows the standard source architecture, utilizing a stack of specialized components for its operation. Pipeline focuses on asynchronous mapping through UniProt ID Mapping API.

| Component | Implementation |
|---|---|
| **Client** | `src/bioetl/integrations/uniprot/client/uniprot_idmapping_client.py` — HTTP client for UniProt ID Mapping API |
| **Parser** | `src/bioetl/integrations/uniprot/parser/idmapping_parser.py` — parsing helpers for ID mapping results |
| **Normalizer** | `src/bioetl/integrations/uniprot/normalizer/idmapping_normalizer.py` — dataframe normalisation and one-to-many handling |
| **JobManager** | `src/bioetl/integrations/uniprot/job/idmapping_job_manager.py` — управление асинхронными job (create, poll, stream) |
| **Schema** | `src/bioetl/schemas/uniprot/mapping/chembl2uniprot_mapping_output_schema.py` — Pandera schema для валидации |

**Public API:**

- `from bioetl.integrations.uniprot.idmapping import UniProtIDMappingClient`
- `from bioetl.integrations.uniprot.idmapping import IDMappingJobManager`
- `from bioetl.pipelines.uniprot import ChEMBL2UniProtMappingPipeline`

**Module layout:**

- `src/bioetl/integrations/uniprot/client/uniprot_idmapping_client.py` — HTTP client для ID Mapping API
- `src/bioetl/integrations/uniprot/job/idmapping_job_manager.py` — управление async job (create, poll, stream)
- `src/bioetl/pipelines/uniprot/chembl2uniprot.py` — standalone CLI pipeline wrapper

**Tests:**

- `tests/bioetl/integration/uniprot/test_idmapping_client.py` — HTTP client adapters для ID Mapping API
- `tests/bioetl/integration/uniprot/test_idmapping_job_manager.py` — job management tests
- `tests/bioetl/integration/uniprot/test_idmapping_parser.py` — parsing helpers tests
- `tests/bioetl/integration/uniprot/test_idmapping_normalizer.py` — normalization tests
- `tests/bioetl/integration/uniprot/test_chembl2uniprot_pipeline_e2e.py` — pipeline orchestration happy path

## 7. Key Identifiers

- **Business Key**: Составной ключ `(target_chembl_id, uniprot_accession)` — уникальная комбинация ChEMBL ID и UniProt accession
- **Sort Key**: `["target_chembl_id", "uniprot_accession"]` — используется для детерминированной сортировки перед записью

## 8. Детерминизм

**Sort keys:** `["target_chembl_id", "uniprot_accession"]`

ChEMBL2UniProt Mapping pipeline обеспечивает детерминированный вывод через стабильную сортировку и хеширование:

- **Sort keys:** Строки сортируются сначала по `target_chembl_id`, затем по `uniprot_accession` перед записью
- **Hash policy:** Используется SHA256 для генерации `hash_row` и `hash_business_key`
  - `hash_row`: хеш всей строки (кроме полей `generated_at`, `run_id`)
  - `hash_business_key`: хеш составного бизнес-ключа `(target_chembl_id, uniprot_accession)`
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
| **Mapping coverage** | 100% идентификаторов | HIGH |
| **Mapping success rate** | ≥90% успешных маппингов | HIGH |
| **One-to-many detection** | 100% обнаружение множественных маппингов | MEDIUM |
| **Duplicate detection** | 0% дубликатов составного ключа | CRITICAL |
| **Pipeline failure rate** | 0% (graceful degradation) | CRITICAL |
| **Детерминизм** | Бит-в-бит воспроизводимость | CRITICAL |

**QC метрики:**

- Покрытие маппинга: процент успешно обработанных target_chembl_id
- Успешность маппинга: процент target_chembl_id с успешным маппингом (status="success")
- One-to-many ratio: процент target_chembl_id с множественными маппингами
- Обнаружение дубликатов: проверка отсутствия дубликатов составного ключа
- Валидность данных: соответствие схеме Pandera

**Пороги качества:**

- Mapping coverage должен быть 100% (критично)
- Mapping success rate ≥90% (высокий приоритет)
- One-to-many detection: все множественные маппинги должны быть обнаружены (средний приоритет)
- Duplicate detection: 0% дубликатов составного ключа (критично)

**QC отчеты:**

- Генерируется `chembl2uniprot_mapping_quality_report.csv` с метриками покрытия и валидности
- При использовании `--extended` режима дополнительно создается подробный отчет с:
  - Распределением one-to-many маппингов
  - Распределением confidence scores
  - Статистикой по mapping_status

For detailed QC metrics and policies, see [QC Overview](../qc/00-qc-overview.md).

## 10. Логирование и трассировка

ChEMBL2UniProt Mapping pipeline использует `UnifiedLogger` для структурированного логирования всех операций с обязательными полями контекста.

**Обязательные поля в логах:**

- `run_id`: Уникальный идентификатор запуска пайплайна
- `stage`: Текущая стадия выполнения (`extract`, `transform`, `validate`, `write`)
- `pipeline`: Имя пайплайна (`chembl2uniprot-mapping`)
- `duration`: Время выполнения стадии в секундах
- `row_count`: Количество обработанных строк

**Структурированные события:**

- `pipeline_started`: Начало выполнения пайплайна
- `idmapping_job_created`: Создание ID mapping job (с job_id)
- `idmapping_job_polling`: Опрос статуса job
- `idmapping_job_completed`: Завершение job (status="FINISHED")
- `idmapping_streaming_started`: Начало потоковой загрузки результатов
- `idmapping_streaming_completed`: Завершение потоковой загрузки результатов
- `extract_started`: Начало стадии извлечения
- `extract_completed`: Завершение стадии извлечения с метриками
- `transform_started`: Начало стадии трансформации
- `transform_completed`: Завершение стадии трансформации (с обработкой one-to-many)
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
  "pipeline": "chembl2uniprot-mapping",
  "timestamp": "2025-01-15T10:30:00.123456Z"
}

{
  "event": "idmapping_job_created",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "chembl2uniprot-mapping",
  "job_id": "job_id_abc123",
  "input_count": 1250,
  "timestamp": "2025-01-15T10:30:05.234567Z"
}

{
  "event": "idmapping_job_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "chembl2uniprot-mapping",
  "job_id": "job_id_abc123",
  "job_status": "FINISHED",
  "polling_attempts": 12,
  "duration": 65.3,
  "timestamp": "2025-01-15T10:31:10.345678Z"
}

{
  "event": "extract_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "chembl2uniprot-mapping",
  "duration": 125.5,
  "row_count": 1320,
  "mapping_success_rate": 94.5,
  "one_to_many_count": 45,
  "one_to_many_ratio": 3.6,
  "timestamp": "2025-01-15T10:32:05.456789Z"
}

{
  "event": "pipeline_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "bootstrap",
  "pipeline": "chembl2uniprot-mapping",
  "duration": 180.2,
  "row_count": 1320,
  "timestamp": "2025-01-15T10:33:00.567890Z"
}
```

**Формат вывода:**

- Консоль: текстовый формат для удобства чтения
- Файлы: JSON формат для машинной обработки и анализа
- Ротация: автоматическая ротация лог-файлов (10MB × 10 файлов)

**Трассировка:**

- Все операции связаны через `run_id` для отслеживания полного жизненного цикла пайплайна
- Каждая стадия логирует начало и завершение с метриками производительности
- Асинхронные операции (job creation, polling) логируются с `job_id` для трассировки
- Ошибки логируются с полным контекстом и stack trace

For detailed logging configuration and API, see [Logging Overview](../logging/00-overview.md).

## 11. Асинхронный процесс маппинга

### 11.1 Обзор

UniProt ID Mapping API использует асинхронный процесс для обработки больших объемов идентификаторов:

1. **Создание job:** Отправка списка идентификаторов через `/idmapping/run`
2. **Polling статуса:** Опрос статуса job через `/idmapping/status` с `job_id`
3. **Streaming результатов:** Загрузка результатов через `/idmapping/stream` после завершения job

### 11.2 Создание ID Mapping Job

**Эндпоинт:** `POST /idmapping/run`

**Параметры запроса:**

```json
{
  "ids": ["CHEMBL240", "CHEMBL231", "CHEMBL232", ...],
  "from": "CHEMBL_ID",
  "to": "UniProtKB"
}
```

**Ответ:**

```json
{
  "jobId": "job_id_abc123"
}
```

**Обработка:**

- Job ID сохраняется для последующего polling
- Логируется событие `idmapping_job_created` с `job_id` и `input_count`

### 11.3 Polling статуса Job

**Эндпоинт:** `GET /idmapping/status/{job_id}`

**Ответ:**

```json
{
  "jobId": "job_id_abc123",
  "status": "RUNNING"  // или "FINISHED", "FAILED"
}
```

**Обработка:**

- Polling выполняется с интервалом `sources.uniprot_idmapping.polling.interval_sec` (по умолчанию 5 секунд)
- Максимальное количество попыток: `sources.uniprot_idmapping.polling.max_polling_attempts` (по умолчанию 60)
- Таймаут: `sources.uniprot_idmapping.polling.timeout_sec` (по умолчанию 300 секунд)
- Логируется событие `idmapping_job_polling` при каждой попытке опроса
- При `status="FINISHED"` логируется событие `idmapping_job_completed` и переход к streaming

### 11.4 Streaming результатов

**Эндпоинт:** `GET /idmapping/stream/{job_id}?format=json`

**Обработка:**

- Результаты загружаются потоковым способом для больших объемов
- Формат: JSON (конфигурируемо)
- Логируется событие `idmapping_streaming_started` в начале загрузки
- Логируется событие `idmapping_streaming_completed` после завершения загрузки с `row_count`

**Формат результата:**

```json
{
  "results": [
    {
      "from": "CHEMBL240",
      "to": "P12345"
    },
    {
      "from": "CHEMBL231",
      "to": "P67890"
    },
    {
      "from": "CHEMBL231",
      "to": "P67891"  // One-to-many mapping
    }
  ],
  "failedIds": []
}
```

### 11.5 Обработка One-to-Many маппингов

**Важно:** Один ChEMBL ID может маппиться на несколько UniProt accession (изоформы).

**Обработка:**

1. **Обнаружение:** Группировка результатов по `from` (target_chembl_id)
2. **Подсчет:** Подсчет количества маппингов для каждого `target_chembl_id`
3. **Маркировка:** Установка `is_primary_mapping=true` для первого маппинга (или на основе confidence_score)
4. **Запись:** Все маппинги записываются как отдельные строки с одинаковым `target_chembl_id`

**Пример:**

```python
# Псевдокод обработки one-to-many
def process_one_to_many(mapping_results: list[dict]) -> pd.DataFrame:
    """Process one-to-many mappings."""
    df = pd.DataFrame(mapping_results)

    # Подсчет маппингов для каждого target_chembl_id
    mapping_counts = df.groupby("target_chembl_id").size()
    df["total_mappings_count"] = df["target_chembl_id"].map(mapping_counts)

    # Сортировка по confidence_score (если доступен) для определения primary
    df = df.sort_values(["target_chembl_id", "confidence_score"], ascending=[True, False])
    df["is_primary_mapping"] = ~df.duplicated(subset=["target_chembl_id"], keep="first")

    return df
```

### 11.6 Fallback механизмы

**Условия активации:**

- HTTP 5xx ошибки при создании job
- Таймауты polling (превышение max_polling_attempts или timeout_sec)
- Job статус "FAILED"
- Ошибки streaming

**Fallback запись:**

```python
def create_fallback_record(target_chembl_id: str, error: Exception = None) -> dict:
    """Create fallback record for failed mapping."""
    return {
        "target_chembl_id": target_chembl_id,
        "uniprot_accession": None,  # NULL для неудачного маппинга
        "mapping_status": "failed",
        "mapping_source": "UniProt_IDMapping_FALLBACK",
        "source_system": "UniProt_IDMapping_FALLBACK",
        "error_code": error.code if hasattr(error, 'code') else None,
        "error_message": str(error) if error else "Fallback: ID Mapping unavailable",
        # ... остальные поля с NULL/default значениями
    }
```
