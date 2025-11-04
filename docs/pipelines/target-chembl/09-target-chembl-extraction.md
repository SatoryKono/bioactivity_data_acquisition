# ChEMBL Target Extraction Pipeline

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

This document describes the `target` pipeline, which is responsible for extracting and processing target data from the ChEMBL database.

## 1. Overview

The `target` pipeline extracts information about macromolecular targets of bioactive compounds from the ChEMBL database. This data is essential for understanding drug-target interactions and mechanisms of action. The pipeline focuses solely on extracting and normalizing target data from ChEMBL API.

**Note:** Enrichment from external sources (UniProt, IUPHAR) is handled by separate pipelines. This pipeline only extracts data from ChEMBL.

## 2. CLI Command

The pipeline is executed via the `target` CLI command.

**Usage:**

```bash
python -m bioetl.cli.main target [OPTIONS]
```

**Example:**

```bash
python -m bioetl.cli.main target \
  --config configs/pipelines/chembl/target.yaml \
  --output-dir data/output/target
```

## 3. Configuration

### 3.1 Обзор конфигурации

Target pipeline управляется через декларативный YAML-файл конфигурации. Все конфигурационные файлы валидируются во время выполнения против строго типизированных Pydantic-моделей, что гарантирует корректность параметров перед запуском пайплайна.

**Расположение конфига:** `configs/pipelines/chembl/target.yaml`

**Профили по умолчанию:** Конфигурация наследует от `configs/profiles/base.yaml` и `configs/profiles/determinism.yaml` через `extends`.

**Основной источник:** ChEMBL API `/target.json` endpoint.

### 3.2 Структура конфигурации

Конфигурационный файл Target pipeline следует стандартной структуре `PipelineConfig`:

```yaml
# configs/pipelines/chembl/target.yaml

version: 1  # Версия схемы конфигурации

extends:  # Профили для наследования
  - ../profiles/base.yaml
  - ../profiles/determinism.yaml

# -----------------------------------------------------------------------------
# Метаданные пайплайна
# -----------------------------------------------------------------------------
pipeline:
  name: "target"
  version: "1.0.0"
  owner: "chembl-team"
  description: "Extract and normalize ChEMBL target metadata"

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
      max_calls: 10
      period: 1.0
    rate_limit_jitter: true
    headers:
      User-Agent: "BioETL/1.0 (UnifiedAPIClient)"
      Accept: "application/json"
  
  # Именованный профиль для ChEMBL
  profiles:
    chembl:
      timeout_sec: 30.0
      retries:
        total: 7
      rate_limit:
        max_calls: 15
        period: 1.0

# -----------------------------------------------------------------------------
# Кэширование
# -----------------------------------------------------------------------------
cache:
  enabled: true
  directory: "http_cache"
  ttl: 86400  # 24 часа
  namespace: "chembl"  # Обеспечивает release-scoped invalidation

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
  chembl:
    enabled: true
    description: "ChEMBL Data Web Services API"
    http_profile: "chembl"  # Ссылка на именованный HTTP-профиль
    batch_size: 25  # КРИТИЧЕСКИ: <= 25 (жёсткое ограничение URL длины)
    max_url_length: 2000  # Максимальная длина URL для предиктивного троттлинга
    parameters:
      endpoint: "/target.json"
      base_url: "https://www.ebi.ac.uk/chembl/api/data"

# -----------------------------------------------------------------------------
# Детерминизм
# -----------------------------------------------------------------------------
determinism:
  enabled: true
  hash_policy_version: "1.0.0"
  float_precision: 6
  datetime_format: "iso8601"
  column_validation_ignore_suffixes: ["_scd", "_temp", "_meta", "_tmp"]
  
  # Ключи сортировки (обязательно: первый ключ - target_chembl_id)
  sort:
    by: ["target_chembl_id"]
    ascending: [true]
    na_position: "last"
  
  # Фиксированный порядок колонок (из TargetSchema.Config.column_order)
  column_order:
    - "target_chembl_id"
    - "pref_name"
    - "organism"
    - "target_type"
    # ... остальные колонки в порядке из TargetSchema.Config.column_order
  
  # Хеширование
  hashing:
    algorithm: "sha256"
    row_fields: []  # Все колонки из column_order (кроме exclude_fields)
    business_key_fields: ["target_chembl_id"]
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
  schema_in: "bioetl.schemas.chembl.target.TargetInputSchema"  # Опционально
  schema_out: "bioetl.schemas.chembl.target.TargetOutputSchema"  # Обязательно
  strict: true  # Строгая проверка порядка колонок
  coerce: true  # Приведение типов в Pandera

# -----------------------------------------------------------------------------
# Материализация
# -----------------------------------------------------------------------------
materialization:
  root: "data/output"
  default_format: "parquet"
  pipeline_subdir: "target"
  filename_template: "target_{date_tag}.{format}"

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
| `sources.chembl.batch_size` | `25` | Жёсткое ограничение длины URL в ChEMBL API (~2000 символов) | `if batch_size > 25: raise ConfigValidationError` |
| `sources.chembl.max_url_length` | `2000` | Максимальная длина URL для предиктивного троттлинга | `<= 2000` |
| `determinism.sort.by[0]` | `"target_chembl_id"` | Первый ключ сортировки должен быть бизнес-ключом | Обязательно |
| `determinism.column_order` | Полный список колонок | Полный список колонок из `TargetSchema.Config.column_order` | Проверяется на соответствие схеме |
| `validation.schema_out` | `"bioetl.schemas.chembl.target.TargetOutputSchema"` | Обязательная ссылка на Pandera-схему | Должен существовать и быть импортируемым |

### 3.4 Валидация конфигурации

Конфигурация валидируется через Pydantic-модель `PipelineConfig` при загрузке:

1. **Типобезопасность:** Все значения проверяются на соответствие типам
2. **Обязательные поля:** Отсутствие обязательных полей приводит к ошибке
3. **Неизвестные ключи:** Неизвестные ключи запрещены (`extra="forbid"`)
4. **Кросс-полевые инварианты:** Проверка согласованности (например, длина `sort.by` и `sort.ascending`)

**Пример ошибки валидации:**

```text
1 validation error for PipelineConfig
sources.chembl.batch_size
  Value error, sources.chembl.batch_size must be <= 25 due to ChEMBL API URL length limit
```

### 3.5 Переопределения через CLI

Параметры конфигурации могут быть переопределены через CLI флаг `--set`:

```bash
python -m bioetl.cli.main target \
  --config configs/pipelines/chembl/target.yaml \
  --output-dir data/output/target \
  --set sources.chembl.batch_size=20 \
  --set determinism.sort.by='["target_chembl_id"]' \
  --set cache.ttl=7200
```

### 3.6 Переменные окружения

Наивысший приоритет имеют переменные окружения (формат: `BIOETL__<SECTION>__<KEY>__<SUBKEY>`):

```bash
export BIOETL__SOURCES__CHEMBL__BATCH_SIZE=25
export BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=90
export BIOETL__DETERMINISM__FLOAT_PRECISION=4
```

### 3.7 Пример полного конфига

Полный пример конфигурационного файла для target pipeline доступен в `configs/pipelines/chembl/target.yaml`. Конфигурация включает все необходимые секции для работы пайплайна с детерминизмом, валидацией и извлечением данных из ChEMBL.

For detailed configuration structure and API, see [Typed Configurations and Profiles](../configs/00-typed-configs-and-profiles.md).

## 4. Data Schemas

### 4.1 Обзор

Target pipeline использует Pandera для строгой валидации данных перед записью. Схема валидации определяет структуру, типы данных, порядок колонок и ограничения для всех записей. Подробности о политике Pandera схем см. в [Pandera Schema Policy](../schemas/00-pandera-policy.md).

**Расположение схемы:** `src/bioetl/schemas/chembl/target/target_output_schema.py`

**Ссылка в конфиге:** `validation.schema_out: "bioetl.schemas.chembl.target.TargetOutputSchema"`

**Версионирование:** Схема имеет семантическую версию (`MAJOR.MINOR.PATCH`), которая фиксируется в `meta.yaml` для каждой записи пайплайна.

### 4.2 Требования к схеме

Схема валидации для target pipeline должна соответствовать следующим требованиям:

1. **Строгость:** `strict=True` - все колонки должны быть явно определены
2. **Приведение типов:** `coerce=True` - автоматическое приведение типов данных
3. **Порядок колонок:** `ordered=True` - фиксированный порядок колонок
4. **Nullable dtypes:** Использование nullable dtypes (`pd.StringDtype()`, `pd.Int64Dtype()`, `pd.Float64Dtype()`) вместо `object`
5. **Бизнес-ключ:** Валидация уникальности `target_chembl_id`

### 4.3 Структура схемы

Ниже приведена структура Pandera схемы для target pipeline:

```python
# src/bioetl/schemas/chembl/target/target_output_schema.py

import pandera as pa
from pandera.typing import Series, DateTime, String, Int64, Float64
from typing import Optional

# Версия схемы
SCHEMA_VERSION = "1.0.0"

class TargetOutputSchema(pa.DataFrameModel):
    """Pandera schema for ChEMBL target output data."""
    
    # Бизнес-ключ (обязательное поле, NOT NULL)
    target_chembl_id: Series[str] = pa.Field(
        description="ChEMBL target identifier",
        nullable=False,
        regex="^CHEMBL\\d+$"
    )
    
    # Основные поля target
    pref_name: Series[str] = pa.Field(
        description="Preferred target name",
        nullable=True
    )
    organism: Series[str] = pa.Field(
        description="Target organism",
        nullable=True
    )
    target_type: Series[str] = pa.Field(
        description="Type of target",
        nullable=True
    )
    species_group_flag: Series[Int64] = pa.Field(
        description="Species group flag",
        nullable=True
    )
    tax_id: Series[Int64] = pa.Field(
        description="Taxonomy ID",
        nullable=True
    )
    component_count: Series[Int64] = pa.Field(
        description="Number of components",
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
        description="Source system (ChEMBL or ChEMBL_FALLBACK)",
        nullable=False,
        isin=["ChEMBL", "ChEMBL_FALLBACK"]
    )
    chembl_release: Series[str] = pa.Field(
        description="ChEMBL release version",
        nullable=False
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
            "target_chembl_id",
            "pref_name",
            "organism",
            "target_type",
            "species_group_flag",
            "tax_id",
            "component_count",
            # ... остальные колонки в фиксированном порядке
            "run_id",
            "git_commit",
            "config_hash",
            "pipeline_version",
            "source_system",
            "chembl_release",
            "extracted_at",
            "hash_row",
            "hash_business_key",
            "index"
        ]
    
    # Валидация уникальности бизнес-ключа
    @pa.check("target_chembl_id")
    def check_unique_target_id(cls, series: Series[str]) -> Series[bool]:
        """Validate uniqueness of target_chembl_id."""
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

1. **Хранение:** Golden CSV/Parquet и `meta.yaml` находятся в `tests/golden/target/`
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

- Поля для фильтрации по типу target или organism (если поддерживается)

**Схема валидации входных данных:**

```python
# src/bioetl/schemas/chembl/target/target_input_schema.py

class TargetInputSchema(pa.DataFrameModel):
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

Выходной файл содержит все поля из `TargetOutputSchema` в фиксированном порядке колонок, определенном в схеме.

**Обязательные артефакты:**

- `target_{date_tag}.csv` или `target_{date_tag}.parquet` — основной датасет с данными target
- `target_{date_tag}_quality_report.csv` — QC метрики и отчет о качестве данных

**Опциональные артефакты (extended режим):**

- `target_{date_tag}_meta.yaml` — метаданные запуска пайплайна
- `target_{date_tag}_run_manifest.json` — манифест запуска (опционально)

**Формат имен файлов:**

- Дата-тег: `YYYYMMDD` (например, `20250115`)
- Формат: определяется параметром `materialization.default_format` (по умолчанию `parquet`)
- Пример: `target_20250115.parquet`, `target_20250115_quality_report.csv`

**Структура выходных данных:**

Выходной файл содержит следующие группы полей:

1. **Бизнес-ключ:** `target_chembl_id`
2. **Основные поля target:** `pref_name`, `organism`, `target_type`, `species_group_flag`, `tax_id`, `component_count`
3. **Системные метаданные:** `run_id`, `git_commit`, `config_hash`, `pipeline_version`, `source_system`, `chembl_release`, `extracted_at`
4. **Хеши:** `hash_row`, `hash_business_key`
5. **Индекс:** `index`

**Пример структуры выходного файла:**

```csv
target_chembl_id,pref_name,organism,target_type,species_group_flag,tax_id,component_count,...,run_id,git_commit,config_hash,pipeline_version,source_system,chembl_release,extracted_at,hash_row,hash_business_key,index
CHEMBL240,Cytochrome P450 2D6,Homo sapiens,PROTEIN,0,9606,1,...,a1b2c3d4e5f6g7h8,abc123...,def456...,1.0.0,ChEMBL,CHEMBL_36,2025-01-15T10:30:00Z,abc123...,def456...,0
```

## 6. Component Architecture

The `target` pipeline follows the standard source architecture, utilizing a stack of specialized components for its operation. Pipeline focuses solely on extracting data from ChEMBL API.

| Component | Implementation |
|---|---|
| **Client** | `[ref: repo:src/bioetl/sources/chembl/target/client/target_client.py@refactoring_001]` |
| **Parser** | `[ref: repo:src/bioetl/sources/chembl/target/parser/target_parser.py@refactoring_001]` |
| **Normalizer** | `[ref: repo:src/bioetl/sources/chembl/target/normalizer/target_normalizer.py@refactoring_001]` |
| **Schema** | `[ref: repo:src/bioetl/schemas/chembl/target/target_output_schema.py@refactoring_001]` |

**Note:** Enrichment from external sources (UniProt, IUPHAR) is handled by separate pipelines. This pipeline only extracts data from ChEMBL.

## 7. Key Identifiers

- **Business Key**: `target_chembl_id` — уникальный идентификатор target из ChEMBL
- **Sort Key**: `target_chembl_id` — используется для детерминированной сортировки перед записью

## 8. Детерминизм

### 8.1 Обзор

Target pipeline обеспечивает детерминированный вывод через стабильную сортировку и хеширование. Это гарантирует бит-в-бит воспроизводимость при одинаковых входных данных и конфигурации.

**Sort keys:** `["target_chembl_id"]`

### 8.2 Стабильная сортировка

Перед записью все строки сортируются по ключу `target_chembl_id` в стабильном порядке:

```python
# Псевдокод в стадии write
sort_keys = config.determinism.sort.by  # ["target_chembl_id"]
df = df.sort_values(
    by=sort_keys,
    kind="stable",  # Стабильная сортировка
    na_position="last"  # NULL значения в конце
).reset_index(drop=True)
```

**Инварианты:**

- Первый ключ сортировки всегда `target_chembl_id` (бизнес-ключ)
- Используется стабильная сортировка (`kind="stable"`)
- NULL значения всегда располагаются в конце (`na_position="last"`)

### 8.3 Хеширование

**Hash policy:** Используется SHA256 для генерации хешей целостности данных.

**hash_row:** Хеш всей строки данных (кроме полей `generated_at`, `run_id`)

```python
# Псевдокод расчета hash_row
def calculate_hash_row(row: dict, column_order: list[str]) -> str:
    """Calculate hash of entire row excluding metadata fields."""
    canonical = {}
    exclude_fields = ["generated_at", "run_id"]
    
    for col in column_order:
        if col in exclude_fields:
            continue
        value = row.get(col)
        # Каноническая нормализация значения
        canonical[col] = canonicalize_value(value)
    
    # Каноническая JSON сериализация
    json_str = json.dumps(canonical, sort_keys=True, separators=(',', ':'))
    return sha256(json_str.encode('utf-8')).hexdigest()
```

**hash_business_key:** Хеш бизнес-ключа (`target_chembl_id`)

```python
# Псевдокод расчета hash_business_key
def calculate_hash_business_key(target_chembl_id: str) -> str:
    """Calculate hash of business key."""
    return sha256(target_chembl_id.encode('utf-8')).hexdigest()
```

### 8.4 Каноническая нормализация

Все значения нормализуются перед хешированием для обеспечения детерминизма:

**Правила канонизации:**

1. **Строки:** trim whitespace, lowercase для идентификаторов
2. **Числа с плавающей точкой:** фиксированная точность (6 знаков после запятой)
3. **Даты/время:** ISO-8601 формат в UTC
4. **NULL/NaN:** пустая строка `""`
5. **Булевы значения:** `"True"` или `"False"` (строки)
6. **JSON объекты/массивы:** каноническая JSON сериализация с `sort_keys=True`

```python
# Псевдокод канонизации
def canonicalize_value(value) -> str:
    """Canonicalize value for deterministic hashing."""
    if pd.isna(value):
        return ""
    elif isinstance(value, float):
        return f"{value:.6f}"
    elif isinstance(value, datetime):
        return value.isoformat() + "Z"
    elif isinstance(value, bool):
        return "True" if value else "False"
    elif isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(',', ':'))
    else:
        return str(value).strip().lower() if is_identifier(value) else str(value).strip()
```

### 8.5 Фиксированный порядок колонок

Порядок колонок фиксируется из Pandera схемы (`TargetSchema.Config.column_order`):

```python
# Псевдокод обеспечения порядка колонок
column_order = config.determinism.column_order  # Из схемы
df = df[column_order]  # Переупорядочивание колонок
```

**Инвариант:** Порядок колонок должен полностью соответствовать `column_order` из схемы.

### 8.6 Атомарная запись файлов

Файлы записываются атомарно для предотвращения частичных или поврежденных файлов:

```python
# Псевдокод атомарной записи
def atomic_write(df: pd.DataFrame, target_path: Path, run_id: str):
    """Write DataFrame atomically."""
    # 1. Временный файл в run_id-scoped директории
    temp_dir = target_path.parent / ".tmp" / run_id
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_path = temp_dir / f"{target_path.name}.tmp"
    
    # 2. Запись во временный файл
    df.to_parquet(temp_path, index=False)
    
    # 3. Atomic rename (fsync + rename)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(str(temp_path), str(target_path))  # Atomic на всех платформах
    
    # 4. Очистка временной директории (если пуста)
    try:
        temp_dir.rmdir()
    except OSError:
        pass
```

### 8.7 Метаданные в meta.yaml

Файл `meta.yaml` содержит полную информацию о детерминизме:

```yaml
# target_{date_tag}_meta.yaml
pipeline_version: "1.0.0"
chembl_release: "CHEMBL_36"
row_count: 1250

# Хеши и детерминизм
hash_algo: "sha256"
hash_policy_version: "1.0.0"
determinism:
  sort_keys: ["target_chembl_id"]
  column_order: [...]  # Полный список колонок
  float_precision: 6
  datetime_format: "iso8601"
  timezone: "UTC"
  locale: "C"

# Checksums артефактов
checksums:
  csv: sha256: "abc123..."
  quality_report: sha256: "def456..."
  meta: sha256: "ghi789..."

# Временные метки
generated_at_utc: "2025-01-15T10:32:00.678901Z"
```

### 8.8 Гарантии детерминизма

Target pipeline гарантирует:

1. **Бит-в-бит воспроизводимость:** При одинаковых входных данных и конфигурации выходные файлы идентичны бит-в-бит
2. **Стабильный порядок строк:** Строки всегда сортируются по `target_chembl_id` в одинаковом порядке
3. **Стабильный порядок колонок:** Колонки всегда в фиксированном порядке из схемы
4. **Идентичные хеши:** Для идентичных данных генерируются идентичные хеши (`hash_row`, `hash_business_key`)
5. **Атомарная запись:** Файлы записываются атомарно, предотвращая частичные записи
6. **Воспроизводимые метаданные:** Все метаданные фиксируются в `meta.yaml` для полной трассируемости

For detailed policy, see [Determinism Policy](../determinism/00-determinism-policy.md).

## 9. QC/QA

### 9.1 Обзор

Target pipeline выполняет комплексную проверку качества данных на всех этапах обработки. QC метрики обеспечивают валидность данных, полноту извлечения и соответствие схемам.

### 9.2 Ключевые метрики успеха

| Метрика | Target | Критичность | Порог |
|---------|--------|-------------|-------|
| **ChEMBL coverage** | 100% идентификаторов | CRITICAL | 100% |
| **Completeness** | Заполненность ключевых полей | HIGH | ≥85% |
| **Uniqueness** | Отсутствие дубликатов | CRITICAL | 0% |
| **Schema compliance** | Соответствие Pandera схеме | CRITICAL | 100% |
| **Pipeline failure rate** | Процент неудачных обработок | CRITICAL | 0% |
| **Детерминизм** | Бит-в-бит воспроизводимость | CRITICAL | 100% |

### 9.3 Детальные QC метрики

#### 9.3.1 ChEMBL Coverage

**Описание:** Процент успешно извлеченных `target_chembl_id` из входных данных.

**Расчет:**

```python
chembll_coverage = (successfully_extracted_count / total_input_count) * 100
```

**Порог:** 100% (CRITICAL)

**Действия при нарушении:**

- Если coverage < 100%, пайплайн завершается с ошибкой
- Логируются все неудачные `target_chembl_id` с причинами ошибок
- QC отчет содержит детальную информацию о неудачных извлечениях

#### 9.3.2 Completeness (Полнота данных)

**Описание:** Процент заполненности ключевых полей для каждого target.

**Ключевые поля:**

- `target_chembl_id` — обязательное (100%)
- `pref_name` — желательное (≥90%)
- `organism` — желательное (≥85%)
- `target_type` — желательное (≥80%)
- `tax_id` — опциональное (≥70%)

**Расчет:**

```python
completeness = (non_null_count / total_count) * 100
```

**Порог:** ≥85% для ключевых полей (HIGH)

**Действия при нарушении:**

- Если completeness < порога, логируется предупреждение
- QC отчет содержит распределение полноты по полям
- В extended режиме создается детальный отчет о пропусках

#### 9.3.3 Uniqueness (Уникальность)

**Описание:** Проверка отсутствия дубликатов по `target_chembl_id`.

**Расчет:**

```python
duplicate_count = df["target_chembl_id"].duplicated().sum()
uniqueness_rate = (1 - duplicate_count / total_count) * 100
```

**Порог:** 100% (CRITICAL)

**Действия при нарушении:**

- Если обнаружены дубликаты, пайплайн завершается с ошибкой
- Логируются все дублирующиеся `target_chembl_id`
- QC отчет содержит список всех дубликатов

#### 9.3.4 Schema Compliance (Соответствие схеме)

**Описание:** Процент записей, соответствующих Pandera схеме.

**Расчет:**

```python
schema_errors = schema.validate(df, lazy=True)
schema_compliance = (1 - len(schema_errors) / total_count) * 100
```

**Порог:** 100% (CRITICAL)

**Действия при нарушении:**

- Если schema_compliance < 100%, пайплайн завершается с ошибкой
- Логируются все ошибки валидации схемы
- QC отчет содержит детальную информацию о нарушениях схемы

#### 9.3.5 Data Quality Flags

**Описание:** Проверка качества данных на основе флагов из ChEMBL.

**Флаги качества:**

- `source_system` — должен быть "ChEMBL" или "ChEMBL_FALLBACK"
- `chembl_release` — должен быть валидным релизом ChEMBL
- `extracted_at` — должен быть валидной датой в UTC

**Порог:** 100% для всех флагов (HIGH)

### 9.4 Пороги качества

**Критические пороги (CRITICAL):**

- ChEMBL coverage: 100% — пайплайн завершается при нарушении
- Uniqueness: 100% — пайплайн завершается при нарушении
- Schema compliance: 100% — пайплайн завершается при нарушении
- Pipeline failure rate: 0% — пайплайн завершается при нарушении

**Высокие пороги (HIGH):**

- Completeness ключевых полей: ≥85% — логируется предупреждение
- Data quality flags: 100% — логируется предупреждение

### 9.5 QC отчеты

#### 9.5.1 Структура quality_report.csv

QC отчет содержит следующие метрики:

```csv
metric,value,threshold,status,details
chembll_coverage,100.0,100.0,PASS,Successfully extracted 1250/1250 targets
completeness_pref_name,92.5,85.0,PASS,1156/1250 targets have pref_name
completeness_organism,88.2,85.0,PASS,1102/1250 targets have organism
completeness_target_type,85.6,80.0,PASS,1070/1250 targets have target_type
uniqueness,100.0,100.0,PASS,No duplicates found
schema_compliance,100.0,100.0,PASS,All records comply with schema
pipeline_failure_rate,0.0,0.0,PASS,No failures detected
```

#### 9.5.2 Расширенный QC отчет (extended режим)

При использовании `--extended` режима дополнительно создается детальный отчет с:

- **Распределениями полей:** Гистограммы распределения значений для каждого поля
- **Корреляциями:** Матрица корреляций между числовыми полями
- **Пропусками:** Детальная информация о пропусках по каждому полю
- **Аномалиями:** Выявленные аномалии в данных

**Формат:** `target_{date_tag}_extended_qc_report.json`

### 9.6 Процесс QC валидации

QC валидация выполняется на следующих этапах:

1. **После extract:** Проверка ChEMBL coverage и базовых метрик
2. **После transform:** Проверка completeness и нормализации данных
3. **После validate:** Проверка schema compliance и uniqueness
4. **После write:** Финальная проверка всех метрик и генерация QC отчетов

**Алгоритм валидации:**

```python
# Псевдокод QC валидации
def validate_qc(df: pd.DataFrame, config: PipelineConfig) -> QCResult:
    """Validate data quality metrics."""
    metrics = {}
    
    # 1. ChEMBL coverage
    metrics["chembll_coverage"] = calculate_coverage(df)
    if metrics["chembll_coverage"] < 100.0:
        raise QCError("ChEMBL coverage below threshold")
    
    # 2. Completeness
    metrics["completeness"] = calculate_completeness(df)
    if metrics["completeness"] < config.qc.thresholds.completeness:
        logger.warning("Completeness below threshold")
    
    # 3. Uniqueness
    metrics["uniqueness"] = calculate_uniqueness(df)
    if metrics["uniqueness"] < 100.0:
        raise QCError("Uniqueness violation detected")
    
    # 4. Schema compliance
    metrics["schema_compliance"] = validate_schema(df)
    if metrics["schema_compliance"] < 100.0:
        raise QCError("Schema compliance violation")
    
    # 5. Generate QC report
    generate_qc_report(metrics, config.output_dir)
    
    return QCResult(metrics=metrics, passed=True)
```

### 9.7 Действия при нарушении порогов

**Критические нарушения (CRITICAL):**

- Пайплайн завершается с ошибкой (exit code != 0)
- Все артефакты удаляются (кроме QC отчетов)
- Детальная информация об ошибке логируется
- QC отчет содержит полную информацию о нарушении

**Высокие нарушения (HIGH):**

- Пайплайн продолжает выполнение с предупреждением
- Предупреждение логируется в структурированном виде
- QC отчет содержит информацию о нарушении
- В extended режиме создается детальный отчет

**Средние нарушения (MEDIUM):**

- Пайплайн продолжает выполнение
- Информация логируется как информационное сообщение
- QC отчет содержит информацию о нарушении

For detailed QC metrics and policies, see [QC Overview](../qc/00-qc-overview.md).

## 10. Логирование и трассировка

### 10.1 Обзор

Target pipeline использует `UnifiedLogger` для структурированного логирования всех операций с обязательными полями контекста. Все логи формируются в формате JSON для машинной обработки и анализа.

### 10.2 Обязательные поля в логах

Каждое логируемое событие содержит следующие обязательные поля:

- `event`: Тип события (например, `pipeline_started`, `extract_completed`)
- `run_id`: Уникальный идентификатор запуска пайплайна
- `stage`: Текущая стадия выполнения (`extract`, `transform`, `validate`, `write`)
- `pipeline`: Имя пайплайна (`target`)
- `timestamp`: Временная метка события в формате ISO-8601 UTC

**Дополнительные поля (по стадиям):**

- `duration`: Время выполнения стадии в секундах (для завершения стадий)
- `row_count`: Количество обработанных строк
- `api_calls`: Количество API вызовов (для extract стадии)
- `cache_hits`: Количество попаданий в кэш (для extract стадии)
- `success_count`: Количество успешных операций
- `error_count`: Количество ошибок

### 10.3 Структурированные события

#### 10.3.1 Полный список событий

| Событие | Стадия | Описание | Обязательные поля |
|---------|--------|----------|-------------------|
| `pipeline_started` | `bootstrap` | Начало выполнения пайплайна | `run_id`, `pipeline`, `timestamp` |
| `extract_started` | `extract` | Начало стадии извлечения | `run_id`, `stage`, `pipeline`, `timestamp` |
| `extract_completed` | `extract` | Завершение стадии извлечения | `run_id`, `stage`, `pipeline`, `duration`, `row_count`, `timestamp` |
| `transform_started` | `transform` | Начало стадии трансформации | `run_id`, `stage`, `pipeline`, `timestamp` |
| `transform_completed` | `transform` | Завершение стадии трансформации | `run_id`, `stage`, `pipeline`, `duration`, `row_count`, `timestamp` |
| `validate_started` | `validate` | Начало валидации | `run_id`, `stage`, `pipeline`, `timestamp` |
| `validate_completed` | `validate` | Завершение валидации | `run_id`, `stage`, `pipeline`, `duration`, `row_count`, `timestamp` |
| `write_started` | `write` | Начало записи результатов | `run_id`, `stage`, `pipeline`, `timestamp` |
| `write_completed` | `write` | Завершение записи результатов | `run_id`, `stage`, `pipeline`, `duration`, `row_count`, `timestamp` |
| `pipeline_completed` | `bootstrap` | Успешное завершение пайплайна | `run_id`, `pipeline`, `duration`, `row_count`, `timestamp` |
| `pipeline_failed` | `*` | Ошибка выполнения | `run_id`, `stage`, `pipeline`, `error`, `timestamp` |

### 10.4 Детальные примеры JSON-логов

#### 10.4.1 Начало выполнения пайплайна

```json
{
  "event": "pipeline_started",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "bootstrap",
  "pipeline": "target",
  "config_path": "configs/pipelines/chembl/target.yaml",
  "output_dir": "data/output/target",
  "timestamp": "2025-01-15T10:30:00.123456Z"
}
```

#### 10.4.2 Начало стадии извлечения

```json
{
  "event": "extract_started",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "target",
  "input_count": 1250,
  "timestamp": "2025-01-15T10:30:00.234567Z"
}
```

#### 10.4.3 Завершение стадии извлечения

```json
{
  "event": "extract_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "target",
  "duration": 45.2,
  "row_count": 1250,
  "success_count": 1245,
  "fallback_count": 5,
  "error_count": 0,
  "api_calls": 50,
  "cache_hits": 1200,
  "chembl_coverage": 100.0,
  "timestamp": "2025-01-15T10:30:45.345678Z"
}
```

#### 10.4.4 Завершение стадии трансформации

```json
{
  "event": "transform_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "transform",
  "pipeline": "target",
  "duration": 12.5,
  "row_count": 1250,
  "normalized_fields": 15,
  "timestamp": "2025-01-15T10:30:57.890123Z"
}
```

#### 10.4.5 Завершение валидации

```json
{
  "event": "validate_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "validate",
  "pipeline": "target",
  "duration": 3.2,
  "row_count": 1250,
  "schema_compliance": 100.0,
  "uniqueness_rate": 100.0,
  "completeness_rate": 89.5,
  "validation_errors": 0,
  "timestamp": "2025-01-15T10:31:01.123456Z"
}
```

#### 10.4.6 Завершение записи результатов

```json
{
  "event": "write_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "write",
  "pipeline": "target",
  "duration": 5.8,
  "row_count": 1250,
  "output_files": [
    "target_20250115.parquet",
    "target_20250115_quality_report.csv"
  ],
  "file_sizes": {
    "target_20250115.parquet": 245678,
    "target_20250115_quality_report.csv": 1234
  },
  "checksums": {
    "target_20250115.parquet": "sha256:abc123...",
    "target_20250115_quality_report.csv": "sha256:def456..."
  },
  "timestamp": "2025-01-15T10:31:07.234567Z"
}
```

#### 10.4.7 Успешное завершение пайплайна

```json
{
  "event": "pipeline_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "bootstrap",
  "pipeline": "target",
  "duration": 67.7,
  "row_count": 1250,
  "total_duration": 67.7,
  "stages": {
    "extract": 45.2,
    "transform": 12.5,
    "validate": 3.2,
    "write": 5.8
  },
  "metrics": {
    "chembl_coverage": 100.0,
    "schema_compliance": 100.0,
    "uniqueness_rate": 100.0,
    "completeness_rate": 89.5
  },
  "timestamp": "2025-01-15T10:31:07.345678Z"
}
```

#### 10.4.8 Ошибка выполнения пайплайна

```json
{
  "event": "pipeline_failed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "target",
  "error": "ChEMBL API unavailable",
  "error_type": "ConnectionError",
  "error_message": "Connection to ChEMBL API timed out after 60 seconds",
  "stack_trace": "Traceback (most recent call last):\n  ...",
  "duration": 62.3,
  "row_count": 0,
  "timestamp": "2025-01-15T10:31:02.456789Z"
}
```

### 10.5 Трассировка

#### 10.5.1 Использование run_id

Все события в рамках одного запуска пайплайна связаны через `run_id`. Это позволяет:

- Отслеживать полный жизненный цикл пайплайна
- Коррелировать события между стадиями
- Анализировать производительность и метрики
- Отлаживать проблемы в конкретном запуске

**Пример трассировки:**

```json
// Все события имеют один и тот же run_id
{"event": "pipeline_started", "run_id": "a1b2c3d4e5f6g7h8", ...}
{"event": "extract_started", "run_id": "a1b2c3d4e5f6g7h8", ...}
{"event": "extract_completed", "run_id": "a1b2c3d4e5f6g7h8", ...}
{"event": "transform_started", "run_id": "a1b2c3d4e5f6g7h8", ...}
{"event": "transform_completed", "run_id": "a1b2c3d4e5f6g7h8", ...}
{"event": "validate_started", "run_id": "a1b2c3d4e5f6g7h8", ...}
{"event": "validate_completed", "run_id": "a1b2c3d4e5f6g7h8", ...}
{"event": "write_started", "run_id": "a1b2c3d4e5f6g7h8", ...}
{"event": "write_completed", "run_id": "a1b2c3d4e5f6g7h8", ...}
{"event": "pipeline_completed", "run_id": "a1b2c3d4e5f6g7h8", ...}
```

#### 10.5.2 Корреляция между стадиями

Стадии логируют начало и завершение с метриками производительности:

- **Начало стадии:** Логирует входные данные и параметры
- **Завершение стадии:** Логирует результаты, метрики и время выполнения
- **Ошибки:** Логируются с полным контекстом и stack trace

**Пример корреляции:**

```json
// Extract stage
{"event": "extract_started", "run_id": "a1b2c3d4e5f6g7h8", "input_count": 1250, ...}
{"event": "extract_completed", "run_id": "a1b2c3d4e5f6g7h8", "row_count": 1250, "duration": 45.2, ...}

// Transform stage (использует результаты extract)
{"event": "transform_started", "run_id": "a1b2c3d4e5f6g7h8", "input_row_count": 1250, ...}
{"event": "transform_completed", "run_id": "a1b2c3d4e5f6g7h8", "row_count": 1250, "duration": 12.5, ...}
```

### 10.6 Конфигурация логирования

#### 10.6.1 Форматы вывода

**Консоль:**

- Текстовый формат для удобства чтения
- Структурированные сообщения с цветовой кодировкой
- Уровни логирования: DEBUG, INFO, WARNING, ERROR, CRITICAL

**Файлы:**

- JSON формат для машинной обработки и анализа
- Один файл на запуск: `target_{run_id}.log`
- Все события в хронологическом порядке

#### 10.6.2 Ротация лог-файлов

- **Максимальный размер:** 10MB на файл
- **Количество файлов:** 10 файлов (100MB общий объем)
- **Стратегия:** Rotating file handler с автоматической ротацией
- **Архивация:** Старые логи сохраняются с суффиксом `.1`, `.2`, и т.д.

#### 10.6.3 Уровни логирования

| Уровень | Использование | Пример |
|---------|---------------|--------|
| `DEBUG` | Детальная отладочная информация | Параметры API запросов, промежуточные результаты |
| `INFO` | Информационные сообщения | Старт/завершение стадий, метрики производительности |
| `WARNING` | Предупреждения | Нарушения порогов QC, но не критичные |
| `ERROR` | Ошибки | Ошибки API, валидации, но пайплайн продолжает работу |
| `CRITICAL` | Критические ошибки | Ошибки, приводящие к остановке пайплайна |

### 10.7 Примеры трассировки полного цикла

**Успешный запуск:**

```text
2025-01-15T10:30:00.123Z [INFO] pipeline_started run_id=a1b2c3d4e5f6g7h8
2025-01-15T10:30:00.234Z [INFO] extract_started run_id=a1b2c3d4e5f6g7h8 input_count=1250
2025-01-15T10:30:45.345Z [INFO] extract_completed run_id=a1b2c3d4e5f6g7h8 duration=45.2 row_count=1250
2025-01-15T10:30:45.456Z [INFO] transform_started run_id=a1b2c3d4e5f6g7h8
2025-01-15T10:30:57.890Z [INFO] transform_completed run_id=a1b2c3d4e5f6g7h8 duration=12.5 row_count=1250
2025-01-15T10:30:57.901Z [INFO] validate_started run_id=a1b2c3d4e5f6g7h8
2025-01-15T10:31:01.123Z [INFO] validate_completed run_id=a1b2c3d4e5f6g7h8 duration=3.2 row_count=1250
2025-01-15T10:31:01.234Z [INFO] write_started run_id=a1b2c3d4e5f6g7h8
2025-01-15T10:31:07.234Z [INFO] write_completed run_id=a1b2c3d4e5f6g7h8 duration=5.8 row_count=1250
2025-01-15T10:31:07.345Z [INFO] pipeline_completed run_id=a1b2c3d4e5f6g7h8 duration=67.7 row_count=1250
```

**Неудачный запуск:**

```text
2025-01-15T10:30:00.123Z [INFO] pipeline_started run_id=a1b2c3d4e5f6g7h8
2025-01-15T10:30:00.234Z [INFO] extract_started run_id=a1b2c3d4e5f6g7h8 input_count=1250
2025-01-15T10:31:02.456Z [ERROR] pipeline_failed run_id=a1b2c3d4e5f6g7h8 error="ChEMBL API unavailable"
```

For detailed logging configuration and API, see [Logging Overview](../logging/00-overview.md).
