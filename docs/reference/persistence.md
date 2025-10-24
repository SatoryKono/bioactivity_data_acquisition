# Persistence & Determinism

## Обзор

Документация по правилам сохранения данных, обеспечению детерминизма и воспроизводимости результатов в ETL пайплайнах bioactivity_data_acquisition.

## Форматы файлов

### CSV (основной формат)

#### Параметры экспорта
```yaml
csv_config:
  encoding: "utf-8"
  float_format: "%.3f"
  date_format: "%Y-%m-%dT%H:%M:%SZ"
  na_rep: ""
  quoting: "csv.QUOTE_MINIMAL"
  lineterminator: "\n"
  escapechar: "\\"
```

#### Пример конфигурации
```python
# src/library/io/csv_writer.py
def write_deterministic_csv(df: pd.DataFrame, filepath: str, config: dict):
    """Запись CSV с детерминированным порядком колонок и строк"""
    
    # 1. Сортировка строк по стабильным ключам
    sort_columns = config.get('determinism.sort.by', ['id'])
    sort_ascending = config.get('determinism.sort.ascending', [True])
    na_position = config.get('determinism.sort.na_position', 'last')
    
    df_sorted = df.sort_values(
        sort_columns, 
        ascending=sort_ascending, 
        na_position=na_position
    )
    
    # 2. Фиксированный порядок колонок
    column_order = config.get('determinism.column_order', df.columns.tolist())
    df_ordered = df_sorted[column_order]
    
    # 3. Нормализация типов данных
    df_normalized = normalize_dataframe_types(df_ordered, config)
    
    # 4. Запись с фиксированными параметрами
    df_normalized.to_csv(
        filepath,
        index=False,
        encoding='utf-8',
        float_format='%.3f',
        date_format='%Y-%m-%dT%H:%M:%SZ',
        na_rep='',
        quoting=csv.QUOTE_MINIMAL,
        lineterminator='\n'
    )
```

### Parquet (опциональный формат)

#### Преимущества
- **Сжатие:** До 10x меньше размер файла
- **Типы данных:** Сохранение точных типов
- **Схема:** Встроенная валидация схемы
- **Производительность:** Быстрое чтение/запись

#### Конфигурация
```yaml
parquet_config:
  compression: "snappy"
  engine: "pyarrow"
  index: false
  schema_validation: true
  preserve_order: true
```

## Именование файлов

### Шаблон именования
```
{entity}_{YYYYMMDD}.{extension}
```

### Примеры
```
documents_20251024.csv
targets_20251024.csv
assays_20251024.csv
activities_20251024.csv
testitems_20251024.csv
```

### Метаданные
```
{entity}_{YYYYMMDD}_meta.yaml
{entity}_{YYYYMMDD}_qc.csv
{entity}_correlation_report_{YYYYMMDD}/
```

## Порядок колонок

### Фиксированный порядок
Порядок колонок определяется в конфигурации `determinism.column_order`:

```yaml
# configs/config_document.yaml
determinism:
  column_order:
    - document_chembl_id
    - title
    - doi
    - journal
    - year
    - volume
    - issue
    - first_page
    - last_page
    - abstract
    - authors
    - publication_date
    - source_system
    - chembl_release
    - extracted_at
    - hash_row
    - hash_business_key
```

### Группировка колонок
1. **Служебные поля:** `index`, `extracted_at`, `hash_*`
2. **Основные метаданные:** `document_chembl_id`, `title`, `doi`
3. **Группированные по источникам:** `chembl_*`, `crossref_*`, `openalex_*`
4. **Валидационные поля:** `valid_*`, `invalid_*`

## Сортировка данных

### Стабильная сортировка
```python
def sort_dataframe_deterministically(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Детерминированная сортировка DataFrame"""
    
    sort_columns = config.get('determinism.sort.by', ['id'])
    sort_ascending = config.get('determinism.sort.ascending', [True])
    na_position = config.get('determinism.sort.na_position', 'last')
    
    # Стабильная сортировка с фиксированным seed
    df_sorted = df.sort_values(
        sort_columns,
        ascending=sort_ascending,
        na_position=na_position,
        kind='mergesort'  # Стабильный алгоритм
    )
    
    return df_sorted
```

### Ключи сортировки по пайплайнам

#### Documents
```yaml
sort_keys:
  - document_chembl_id
  - doi
  - title
```

#### Targets
```yaml
sort_keys:
  - target_chembl_id
  - pref_name
  - organism
```

#### Assays
```yaml
sort_keys:
  - assay_chembl_id
  - target_chembl_id
  - assay_type
```

#### Activities
```yaml
sort_keys:
  - activity_chembl_id
  - assay_chembl_id
  - molecule_chembl_id
  - target_chembl_id
```

#### Testitems
```yaml
sort_keys:
  - molecule_chembl_id
  - pref_name
  - max_phase
```

## NA политика

### Обработка пустых значений
```yaml
na_policy:
  strings: ""
  integers: null
  floats: null
  booleans: null
  datetimes: null
```

### Нормализация NA
```python
def normalize_na_values(df: pd.DataFrame) -> pd.DataFrame:
    """Нормализация пустых значений"""
    
    # Строки: пустая строка
    string_columns = df.select_dtypes(include=['object']).columns
    df[string_columns] = df[string_columns].fillna('')
    
    # Числа: None
    numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
    df[numeric_columns] = df[numeric_columns].where(pd.notna(df[numeric_columns]), None)
    
    # Булевы: None
    bool_columns = df.select_dtypes(include=['bool']).columns
    df[bool_columns] = df[bool_columns].where(pd.notna(df[bool_columns]), None)
    
    return df
```

## Хеширование и контроль целостности

### Хеши строк
```python
def calculate_row_hash(row: pd.Series) -> str:
    """Расчёт MD5 хеша строки"""
    import hashlib
    
    # Сериализация строки в стабильном порядке
    row_str = '|'.join([str(v) if pd.notna(v) else '' for v in row])
    
    # MD5 хеш
    return hashlib.md5(row_str.encode('utf-8')).hexdigest()
```

### Хеши бизнес-ключей
```python
def calculate_business_key_hash(row: pd.Series, key_columns: list) -> str:
    """Расчёт SHA256 хеша бизнес-ключа"""
    import hashlib
    
    # Извлечение ключевых полей
    key_values = [str(row[col]) if pd.notna(row[col]) else '' for col in key_columns]
    key_str = '|'.join(key_values)
    
    # SHA256 хеш
    return hashlib.sha256(key_str.encode('utf-8')).hexdigest()
```

### Контрольные суммы файлов
```python
def calculate_file_checksums(filepath: str) -> dict:
    """Расчёт контрольных сумм файла"""
    import hashlib
    
    checksums = {}
    
    with open(filepath, 'rb') as f:
        content = f.read()
        
        # MD5
        checksums['md5'] = hashlib.md5(content).hexdigest()
        
        # SHA256
        checksums['sha256'] = hashlib.sha256(content).hexdigest()
    
    return checksums
```

## Локаль и таймзона

### Настройки локали
```yaml
locale_config:
  encoding: "utf-8"
  decimal_separator: "."
  thousands_separator: ","
  date_format: "%Y-%m-%dT%H:%M:%SZ"
  timezone: "UTC"
```

### Обработка дат
```python
def normalize_datetime_utc(dt: any) -> str:
    """Нормализация даты в UTC ISO 8601"""
    from datetime import datetime, timezone
    
    if pd.isna(dt):
        return None
    
    if isinstance(dt, str):
        dt = pd.to_datetime(dt)
    
    # Приведение к UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    
    # ISO 8601 формат
    return dt.isoformat().replace('+00:00', 'Z')
```

## Форматирование чисел

### Float формат
```yaml
float_formatting:
  precision: 3
  format: "%.3f"
  scientific_notation: false
  remove_trailing_zeros: false
```

### Примеры форматирования
```python
# Входные значения
values = [1.23456789, 0.000123456, 1234567.89]

# Форматирование
formatted = [f"{v:.3f}" for v in values]
# Результат: ["1.235", "0.000", "1234567.890"]
```

## Meta.yaml структура

### Обязательные поля
```yaml
pipeline:
  name: documents
  version: "2.0.0"
  entity_type: documents
  source_system: chembl

execution:
  run_id: documents_20251024_143022_a1b2c3d4
  started_at: "2025-10-24T14:30:22Z"
  completed_at: "2025-10-24T14:35:18Z"
  duration_sec: 296.5

data:
  row_count: 1500
  row_count_accepted: 1485
  row_count_rejected: 15
  columns_count: 12

sources:
  - name: chembl
    version: ChEMBL_33
    records: 1500
  - name: crossref
    version: "latest"
    records: 1200
  - name: openalex
    version: "latest"
    records: 1000

validation:
  schema_passed: true
  qc_passed: true
  warnings: 3
  errors: 0

files:
  dataset: documents_20251024.csv
  quality_report: documents_20251024_qc.csv
  correlation_report: documents_correlation_report_20251024/

checksums:
  documents_20251024.csv_md5: a1b2c3d4e5f6...
  documents_20251024.csv_sha256: 1a2b3c4d5e6f...
  documents_20251024_qc.csv_md5: f6e5d4c3b2a1...
  documents_20251024_qc.csv_sha256: 6f5e4d3c2b1a...
```

## Воспроизводимость

### Детерминированные настройки
```python
def ensure_deterministic_environment():
    """Обеспечение детерминированной среды выполнения"""
    
    # Фиксированный seed для случайных чисел
    np.random.seed(42)
    random.seed(42)
    
    # Отключение многопоточности в pandas
    pd.set_option('mode.chained_assignment', None)
    
    # Фиксированная локаль
    import locale
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    
    # UTC таймзона
    import os
    os.environ['TZ'] = 'UTC'
```

### Проверка воспроизводимости
```python
def verify_reproducibility(filepath1: str, filepath2: str) -> bool:
    """Проверка идентичности файлов"""
    
    with open(filepath1, 'rb') as f1, open(filepath2, 'rb') as f2:
        return f1.read() == f2.read()
```

## Качество данных

### QC метрики
```yaml
quality_metrics:
  fill_rate:
    threshold: 0.95
    description: "Процент заполненных полей"
  duplicate_rate:
    threshold: 0.01
    description: "Процент дубликатов"
  validation_errors:
    threshold: 0.05
    description: "Процент ошибок валидации"
  schema_compliance:
    threshold: 1.0
    description: "Соответствие схеме"
```

### Автоматические проверки
```python
def run_quality_checks(df: pd.DataFrame, config: dict) -> dict:
    """Запуск проверок качества данных"""
    
    metrics = {}
    
    # Fill rate
    total_cells = df.size
    filled_cells = df.count().sum()
    metrics['fill_rate'] = filled_cells / total_cells
    
    # Duplicate rate
    duplicates = df.duplicated().sum()
    metrics['duplicate_rate'] = duplicates / len(df)
    
    # Validation errors
    validation_errors = df.isnull().sum().sum()
    metrics['validation_errors'] = validation_errors / total_cells
    
    return metrics
```

## Мониторинг

### Метрики производительности
```yaml
performance_metrics:
  processing_time:
    unit: "seconds"
    aggregation: "sum"
  memory_usage:
    unit: "MB"
    aggregation: "max"
  file_size:
    unit: "MB"
    aggregation: "last"
  throughput:
    unit: "records/second"
    aggregation: "mean"
```

### Алерты
```yaml
alerts:
  high_processing_time:
    threshold: 3600  # 1 час
    severity: "warning"
  low_fill_rate:
    threshold: 0.8
    severity: "error"
  high_duplicate_rate:
    threshold: 0.1
    severity: "warning"
  schema_validation_failure:
    threshold: 0
    severity: "error"
```

## Версионирование

### Семантическое версионирование
- **Major:** Изменения схемы данных
- **Minor:** Новые поля или источники
- **Patch:** Исправления ошибок

### Миграция данных
```yaml
migration:
  version: "2.0.0"
  breaking_changes:
    - "Изменение порядка колонок"
    - "Новые обязательные поля"
  backward_compatibility: false
  migration_script: "migrate_v1_to_v2.py"
```

## Лучшие практики

### 1. Детерминизм
- Всегда используйте фиксированный порядок колонок
- Применяйте стабильную сортировку
- Нормализуйте типы данных перед записью

### 2. Производительность
- Используйте пакетную обработку для больших объёмов
- Кэшируйте промежуточные результаты
- Оптимизируйте порядок операций

### 3. Качество
- Валидируйте данные на каждом этапе
- Логируйте все изменения
- Создавайте контрольные суммы

### 4. Мониторинг
- Отслеживайте метрики качества
- Настройте алерты на аномалии
- Ведите историю изменений

### 5. Тестирование
- Тестируйте воспроизводимость
- Проверяйте контрольные суммы
- Валидируйте схемы данных
