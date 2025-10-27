# CSV формат

## Обзор

Данный документ описывает спецификацию формата CSV файлов, генерируемых пайплайном Bioactivity Data Acquisition, включая порядок колонок, форматирование значений и детерминированную сортировку.

## Спецификация формата

### Базовые параметры

**Кодировка**: UTF-8  
**Разделитель**: Запятая (,)  
**Экранирование**: Стандартное CSV экранирование  
**Индекс**: Не включается в файл  

### Порядок колонок

**Источник истины**: `column_order` в YAML конфигурации

**Пример конфигурации**:
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
    - activity_relation
    - assay_type
    - assay_description
    - molecule_name
    - molecule_smiles
    - target_name
    - target_type
```

### Форматирование значений

#### Числовые значения

**Float значения**:
- Точность: 6 знаков после запятой
- Формат: `123.456789`
- Отсутствующие значения: пустая строка

**Integer значения**:
- Формат: `12345`
- Отсутствующие значения: пустая строка

**Пример**:
```python
def format_numeric_value(value) -> str:
    """Форматирует числовое значение для CSV."""
    if pd.isna(value):
        return ""
    elif isinstance(value, float):
        return f"{value:.6f}"
    elif isinstance(value, int):
        return str(value)
    else:
        return str(value)
```

#### Строковые значения

**Обычные строки**:
- Тримминг пробелов
- Экранирование кавычек
- Кодировка UTF-8

**Идентификаторы**:
- Формат: `CHEMBL123456`
- Валидация по regex: `^CHEMBL\d+$`

**Пример**:
```python
def format_string_value(value) -> str:
    """Форматирует строковое значение для CSV."""
    if pd.isna(value):
        return ""
    
    # Тримминг и экранирование
    formatted = str(value).strip()
    
    # Экранирование кавычек
    if '"' in formatted:
        formatted = formatted.replace('"', '""')
    
    return formatted
```

#### Булевы значения

**Формат**: `true` / `false` (строчные)  
**Отсутствующие значения**: пустая строка

**Пример**:
```python
def format_boolean_value(value) -> str:
    """Форматирует булево значение для CSV."""
    if pd.isna(value):
        return ""
    elif isinstance(value, bool):
        return str(value).lower()
    else:
        return ""
```

#### Даты

**Формат**: ISO 8601 (`YYYY-MM-DD`)  
**Отсутствующие значения**: пустая строка

**Пример**:
```python
def format_date_value(value) -> str:
    """Форматирует дату для CSV."""
    if pd.isna(value):
        return ""
    
    try:
        if isinstance(value, str):
            # Парсинг строки даты
            parsed_date = datetime.strptime(value, "%Y-%m-%d")
        elif hasattr(value, 'strftime'):
            # Объект datetime
            parsed_date = value
        else:
            return ""
        
        return parsed_date.strftime("%Y-%m-%d")
    except ValueError:
        return ""
```

## Детерминированная сортировка

### Принципы сортировки

**Стабильные ключи**: `["assay_id", "molecule_id", "activity_id"]`

**Алгоритм**:
1. Сортировка по `assay_id` (лексикографическая)
2. Внутри каждого assay_id по `molecule_id`
3. Внутри каждой пары по `activity_id`

**Пример**:
```python
def sort_dataframe_deterministically(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Сортирует DataFrame детерминированно."""
    
    sort_keys = ["assay_id", "molecule_id", "activity_id"]
    
    # Проверка наличия ключей сортировки
    missing_keys = set(sort_keys) - set(dataframe.columns)
    if missing_keys:
        raise ValueError(f"Отсутствуют ключи сортировки: {missing_keys}")
    
    # Детерминированная сортировка
    sorted_df = dataframe.sort_values(
        by=sort_keys,
        ignore_index=True,
        na_position='last'
    )
    
    return sorted_df
```

### Обработка отсутствующих значений

**Стратегия**: Отсутствующие значения помещаются в конец

**Пример**:
```python
def handle_missing_values_in_sorting(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Обрабатывает отсутствующие значения при сортировке."""
    
    df = dataframe.copy()
    
    # Заполнение отсутствующих значений для сортировки
    for col in ["assay_id", "molecule_id", "activity_id"]:
        if col in df.columns:
            df[col] = df[col].fillna("ZZZ_MISSING")
    
    return df
```

## Генерация CSV

### Основная функция

```python
def write_deterministic_csv(
    dataframe: pd.DataFrame,
    output_path: Path,
    csv_format: CsvFormatSettings
) -> None:
    """Записывает DataFrame в детерминированном CSV формате."""
    
    # Применение порядка колонок
    if csv_format.column_order:
        ordered_df = dataframe[csv_format.column_order]
    else:
        ordered_df = dataframe
    
    # Детерминированная сортировка
    sorted_df = sort_dataframe_deterministically(ordered_df)
    
    # Форматирование значений
    formatted_df = format_dataframe_for_csv(sorted_df)
    
    # Запись в файл
    formatted_df.to_csv(
        output_path,
        index=False,
        encoding=csv_format.encoding,
        sep=csv_format.delimiter
    )
```

### Форматирование DataFrame

```python
def format_dataframe_for_csv(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Форматирует DataFrame для записи в CSV."""
    
    formatted_df = dataframe.copy()
    
    # Форматирование по типам колонок
    for column in formatted_df.columns:
        if formatted_df[column].dtype == 'object':
            # Строковые колонки
            formatted_df[column] = formatted_df[column].apply(format_string_value)
        elif formatted_df[column].dtype in ['int64', 'int32']:
            # Целочисленные колонки
            formatted_df[column] = formatted_df[column].apply(format_integer_value)
        elif formatted_df[column].dtype in ['float64', 'float32']:
            # Числовые колонки
            formatted_df[column] = formatted_df[column].apply(format_numeric_value)
        elif formatted_df[column].dtype == 'bool':
            # Булевы колонки
            formatted_df[column] = formatted_df[column].apply(format_boolean_value)
        elif 'datetime' in str(formatted_df[column].dtype):
            # Колонки с датами
            formatted_df[column] = formatted_df[column].apply(format_date_value)
    
    return formatted_df
```

## Валидация CSV

### Проверка структуры

```python
def validate_csv_structure(file_path: Path, expected_columns: list) -> bool:
    """Проверяет структуру CSV файла."""
    
    try:
        df = pd.read_csv(file_path, nrows=0)  # Только заголовки
        
        # Проверка колонок
        if list(df.columns) != expected_columns:
            print(f"Несоответствие колонок: ожидалось {expected_columns}, получено {list(df.columns)}")
            return False
        
        return True
    except Exception as e:
        print(f"Ошибка чтения CSV: {e}")
        return False
```

### Проверка данных

```python
def validate_csv_data(file_path: Path) -> dict:
    """Проверяет данные в CSV файле."""
    
    validation_results = {}
    
    try:
        df = pd.read_csv(file_path)
        
        # Проверка количества строк
        validation_results["row_count"] = len(df)
        
        # Проверка отсутствующих значений
        missing_counts = df.isnull().sum()
        validation_results["missing_values"] = missing_counts.to_dict()
        
        # Проверка дубликатов
        duplicate_count = df.duplicated().sum()
        validation_results["duplicates"] = duplicate_count
        
        # Проверка типов данных
        validation_results["dtypes"] = df.dtypes.to_dict()
        
    except Exception as e:
        validation_results["error"] = str(e)
    
    return validation_results
```

## Примеры CSV файлов

### Активности

**Файл**: `data/output/activities.csv`

```csv
assay_id,molecule_id,target_id,activity_id,activity_value,activity_type,activity_units,activity_relation
CHEMBL123456,CHEMBL789012,CHEMBL345678,CHEMBL901234,1.234567,IC50,nM,=
CHEMBL123456,CHEMBL789013,CHEMBL345678,CHEMBL901235,2.345678,IC50,nM,=
CHEMBL123457,CHEMBL789012,CHEMBL345679,CHEMBL901236,3.456789,EC50,uM,<
```

### Молекулы

**Файл**: `data/output/molecules.csv`

```csv
molecule_id,molecule_name,molecule_smiles,molecule_inchi,molecular_weight,heavy_atom_count
CHEMBL789012,Aspirin,CC(=O)OC1=CC=CC=C1C(=O)O,InChI=1S/C9H8O4/c1-6(10)13-8-5-3-2-4-7(8)9(11)12/h2-5H,1H3,(H,11,12),180.157,13
CHEMBL789013,Ibuprofen,CC(C)CC1=CC=C(C=C1)C(C)C(=O)O,InChI=1S/C13H18O2/c1-9(2)8-11-4-6-12(7-5-11)10(3)13(14)15/h4-7,9-10H,8H2,1-3H3,(H,14,15),206.285,15
```

### Мишени

**Файл**: `data/output/targets.csv`

```csv
target_id,target_name,target_type,target_organism,uniprot_id,gene_name
CHEMBL345678,Cyclooxygenase-1,SINGLE PROTEIN,Homo sapiens,P23219,PTGS1
CHEMBL345679,Cyclooxygenase-2,SINGLE PROTEIN,Homo sapiens,P35354,PTGS2
```

## Конфигурация CSV

### Настройки в YAML

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
    - activity_relation
  delimiter: ","
  encoding: "utf-8"
  index: false
  float_format: "%.6f"
```

### Программная настройка

```python
from library.config import CsvFormatSettings

# Создание настроек CSV
csv_settings = CsvFormatSettings(
    column_order=[
        "assay_id", "molecule_id", "target_id", 
        "activity_id", "activity_value", "activity_type"
    ],
    delimiter=",",
    encoding="utf-8",
    index=False,
    float_format="%.6f"
)

# Использование настроек
write_deterministic_csv(
    dataframe=df,
    output_path=Path("output.csv"),
    csv_format=csv_settings
)
```

## Обработка ошибок

### Ошибки записи

```python
def safe_csv_write(dataframe: pd.DataFrame, output_path: Path) -> bool:
    """Безопасная запись CSV с обработкой ошибок."""
    
    try:
        # Создание директории если не существует
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Запись во временный файл
        temp_path = output_path.with_suffix('.tmp')
        dataframe.to_csv(temp_path, index=False)
        
        # Атомарное перемещение
        temp_path.replace(output_path)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка записи CSV: {e}")
        return False
```

### Восстановление после ошибок

```python
def recover_from_csv_error(dataframe: pd.DataFrame, output_path: Path) -> None:
    """Восстанавливается после ошибки записи CSV."""
    
    # Попытка записи с упрощенным форматом
    try:
        dataframe.to_csv(
            output_path,
            index=False,
            encoding='utf-8',
            errors='replace'
        )
    except Exception as e:
        # Последняя попытка с минимальными настройками
        dataframe.to_csv(
            output_path,
            index=False,
            encoding='ascii',
            errors='ignore'
        )
```

## Мониторинг и логирование

### Метрики записи

```python
def log_csv_metrics(output_path: Path, dataframe: pd.DataFrame) -> None:
    """Логирует метрики записи CSV."""
    
    metrics = {
        "file_path": str(output_path),
        "file_size": output_path.stat().st_size,
        "row_count": len(dataframe),
        "column_count": len(dataframe.columns),
        "encoding": "utf-8"
    }
    
    logger.info(f"CSV записан: {metrics}")
```

### Проверка целостности

```python
def verify_csv_integrity(file_path: Path) -> bool:
    """Проверяет целостность CSV файла."""
    
    try:
        # Попытка чтения файла
        df = pd.read_csv(file_path)
        
        # Проверка на пустоту
        if df.empty:
            logger.warning(f"CSV файл пуст: {file_path}")
            return False
        
        # Проверка кодировки
        with open(file_path, 'r', encoding='utf-8') as f:
            f.read()
        
        return True
    except Exception as e:
        logger.error(f"Ошибка проверки целостности CSV: {e}")
        return False
```
