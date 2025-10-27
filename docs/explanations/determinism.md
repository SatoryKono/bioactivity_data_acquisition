# Детерминизм в проекте

## Обзор

Детерминизм — это принцип обеспечения воспроизводимости результатов ETL-пайплайна. При одинаковых входных данных и конфигурации пайплайн должен генерировать идентичные выходные файлы.

## Принципы детерминированности

### 1. Стабильная сортировка

**Проблема**: Порядок строк в CSV может изменяться между запусками из-за:
- Порядка обработки источников данных
- Параллельной обработки
- Изменений в структуре данных

**Решение**: Сортировка по стабильным ключам

```python
# Стабильные ключи для сортировки
sort_keys = ["assay_id", "molecule_id", "activity_id"]

# Детерминированная сортировка
sorted_data = dataframe.sort_values(by=sort_keys, ignore_index=True)
```

### 2. Детерминированное хеширование

**Цель**: Контроль целостности данных и отслеживание изменений

**Реализация**:
```python
def generate_deterministic_hash(row: pd.Series) -> str:
    """Генерирует детерминированный хеш для строки данных."""
    # Сортировка полей для стабильности
    sorted_values = sorted(row.dropna().items())
    
    # Создание строки для хеширования
    hash_string = "|".join(f"{k}:{v}" for k, v in sorted_values)
    
    # MD5 хеш
    return hashlib.md5(hash_string.encode()).hexdigest()
```

### 3. Метаданные пайплайна

**Файл**: `meta.yaml`

**Содержимое**:
```yaml
pipeline_version: "1.0.0"
chembl_release: "33"
row_count: 12345
checksums:
  data_csv: "a1b2c3d4e5f6..."
  qc_report: "f6e5d4c3b2a1..."
generated_at: "2024-01-15T10:30:00Z"
config_hash: "config_abc123..."
```

## Механизмы обеспечения детерминированности

### 1. Конфигурация детерминизма

**Настройки в config.yaml**:
```yaml
determinism:
  enable_deterministic_sorting: true
  sort_keys: ["assay_id", "molecule_id", "activity_id"]
  generate_checksums: true
  include_meta_yaml: true
```

### 2. Нормализация данных

**Принципы**:
- Единообразное форматирование полей
- Стандартизация значений NULL/NaN
- Нормализация строковых значений

**Примеры**:
```python
# Нормализация строк
def normalize_string(value: str) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()

# Нормализация числовых значений
def normalize_numeric(value) -> float:
    if pd.isna(value):
        return 0.0
    return float(value)
```

### 3. Обработка временных меток

**Проблема**: Временные метки могут изменяться между запусками

**Решение**: Фиксация времени генерации
```python
# Фиксированное время для всего пайплайна
pipeline_timestamp = datetime.utcnow().isoformat()

# Использование в метаданных
meta_data = {
    "generated_at": pipeline_timestamp,
    "pipeline_version": config.version
}
```

## Детерминированный экспорт CSV

### 1. Порядок колонок

**Источник истины**: `column_order` в YAML конфигурации

```yaml
csv_format:
  column_order:
    - assay_id
    - molecule_id
    - target_id
    - activity_id
    - activity_value
    - activity_type
    - activity_units
```

### 2. Форматирование значений

**Стандартизация**:
- Числовые значения: фиксированная точность
- Строковые значения: нормализованные
- Даты: ISO формат
- Булевы значения: true/false

```python
def format_for_csv(value) -> str:
    """Форматирует значение для детерминированного CSV."""
    if pd.isna(value):
        return ""
    elif isinstance(value, float):
        return f"{value:.6f}"
    elif isinstance(value, bool):
        return str(value).lower()
    else:
        return str(value)
```

### 3. Контроль целостности

**Хеширование файлов**:
```python
def calculate_file_checksum(file_path: Path) -> str:
    """Вычисляет MD5 хеш файла."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
```

## Примеры детерминированного экспорта

### 1. Базовый экспорт

```python
def write_deterministic_csv(
    dataframe: pd.DataFrame,
    output_path: Path,
    config: Config
) -> None:
    """Записывает DataFrame в детерминированном формате."""
    
    # Сортировка по стабильным ключам
    sorted_data = dataframe.sort_values(
        by=config.determinism.sort_keys,
        ignore_index=True
    )
    
    # Применение порядка колонок
    ordered_data = sorted_data[config.csv_format.column_order]
    
    # Форматирование значений
    formatted_data = ordered_data.applymap(format_for_csv)
    
    # Запись в CSV
    formatted_data.to_csv(
        output_path,
        index=False,
        encoding='utf-8'
    )
```

### 2. Генерация метаданных

```python
def generate_meta_yaml(
    dataframe: pd.DataFrame,
    config: Config,
    output_path: Path
) -> None:
    """Генерирует meta.yaml с метаданными пайплайна."""
    
    meta_data = {
        "pipeline_version": config.version,
        "chembl_release": config.sources.chembl.release,
        "row_count": len(dataframe),
        "generated_at": datetime.utcnow().isoformat(),
        "config_hash": calculate_config_hash(config),
        "checksums": {
            "data_csv": calculate_file_checksum(output_path)
        }
    }
    
    with open(output_path.parent / "meta.yaml", "w") as f:
        yaml.dump(meta_data, f, default_flow_style=False)
```

## Валидация детерминированности

### 1. Тестирование воспроизводимости

```python
def test_deterministic_output():
    """Тест воспроизводимости результатов."""
    
    # Первый запуск
    result1 = run_pipeline(config)
    checksum1 = calculate_file_checksum(result1)
    
    # Второй запуск
    result2 = run_pipeline(config)
    checksum2 = calculate_file_checksum(result2)
    
    # Проверка идентичности
    assert checksum1 == checksum2, "Результаты не детерминированы"
```

### 2. Сравнение файлов

```python
def compare_csv_files(file1: Path, file2: Path) -> bool:
    """Сравнивает два CSV файла на идентичность."""
    
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    
    # Проверка структуры
    if not df1.columns.equals(df2.columns):
        return False
    
    # Проверка данных
    return df1.equals(df2)
```

## Обработка недетерминированных источников

### 1. Проблемные источники

**API с изменяющимся порядком**:
- Некоторые API возвращают данные в случайном порядке
- Временные метки в ответах
- Динамические идентификаторы

### 2. Стратегии решения

**Сортировка на уровне клиента**:
```python
def fetch_and_sort_data(client: BaseApiClient) -> pd.DataFrame:
    """Извлекает и сортирует данные для детерминированности."""
    
    raw_data = client.fetch_all()
    
    # Сортировка по стабильным полям
    sorted_data = raw_data.sort_values(
        by=["id", "created_at"],
        ignore_index=True
    )
    
    return sorted_data
```

**Нормализация временных меток**:
```python
def normalize_timestamps(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Нормализует временные метки для детерминированности."""
    
    df = dataframe.copy()
    
    # Замена временных меток на фиксированные
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].dt.strftime('%Y-%m-%d')
    
    return df
```

## Мониторинг детерминированности

### 1. Метрики

- **Время выполнения** — отслеживание производительности
- **Размер выходных файлов** — контроль объема данных
- **Хеши файлов** — проверка целостности
- **Количество строк** — контроль полноты данных

### 2. Алерты

- **Изменение хешей** — уведомление о недетерминированности
- **Изменение размера файлов** — контроль объема данных
- **Ошибки валидации** — проблемы с качеством данных

## Лучшие практики

### 1. Конфигурация

- Используйте стабильные ключи сортировки
- Фиксируйте версии зависимостей
- Документируйте все параметры детерминизма

### 2. Тестирование

- Регулярно проверяйте воспроизводимость
- Автоматизируйте тесты детерминированности
- Мониторьте изменения в выходных данных

### 3. Документирование

- Описывайте все источники недетерминированности
- Ведите журнал изменений в поведении
- Документируйте стратегии обработки проблем

## Примеры проблем и решений

### Проблема: Изменяющийся порядок API ответов

**Симптомы**:
- Разные хеши при одинаковых входных данных
- Изменяющийся порядок строк в CSV

**Решение**:
```python
# Добавление стабильной сортировки
def ensure_deterministic_order(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Обеспечивает детерминированный порядок строк."""
    
    # Сортировка по всем колонкам для полной стабильности
    return dataframe.sort_values(
        by=list(dataframe.columns),
        ignore_index=True
    )
```

### Проблема: Временные метки в данных

**Симптомы**:
- Разные временные метки при каждом запуске
- Нестабильные хеши

**Решение**:
```python
# Нормализация временных меток
def normalize_timestamps(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Нормализует временные метки для детерминированности."""
    
    df = dataframe.copy()
    
    # Замена временных меток на фиксированные значения
    timestamp_columns = df.select_dtypes(include=['datetime64']).columns
    
    for col in timestamp_columns:
        df[col] = "2024-01-01T00:00:00Z"  # Фиксированная метка
    
    return df
```
