# Стандарты унификации пайплайнов

## Обзор

Данный документ описывает стандарты унификации для всех пайплайнов проекта bioactivity_data_acquisition. Цель - обеспечить консистентность, детерминизм и воспроизводимость результатов.

## 1. Маппинг типов данных

### YAML ↔ Pandas ↔ Pandera

| YAML Type | Pandas Type | Pandera Type | Описание |
|-----------|-------------|--------------|----------|
| STRING | object | pa.String | Строковый тип |
| INT | int64 | pa.Int | Целочисленный тип |
| DECIMAL | float64 | pa.Float | Десятичный тип |
| FLOAT | float64 | pa.Float | Число с плавающей точкой |
| BOOL | bool | pa.Bool | Булевый тип |
| TIMESTAMP | datetime64[ns] | pd.Timestamp | Временная метка |
| DATE | datetime64[ns] | pd.Timestamp | Дата |
| TEXT | object | pa.String | Текстовый тип |

### Правила коэрсии

1. **Строки**: Все строковые значения приводятся к типу `object` в pandas
2. **Числа**: Автоматическое приведение с обработкой NaN значений
3. **Даты**: Приведение к `datetime64[ns]` с UTC временной зоной
4. **Булевы**: Приведение к `bool` с обработкой строковых значений

## 2. Стандарты детерминизма

### Порядок колонок

- **Источник**: Строго из `determinism.column_order` в YAML конфигурации
- **Применение**: В `PipelineBase._write_output()` или `<entity>/pipeline.py`
- **Проверка**: Сравнение с фактическим порядком в CSV

### Форматирование чисел

```yaml
io:
  output:
    csv:
      float_format: "%.6f"  # Единый формат для всех пайплайнов
```

### Политика NA/NULL значений

```yaml
io:
  output:
    csv:
      na_rep: ""  # Пустая строка для отсутствующих значений
      line_terminator: "\n"  # Unix line endings
```

### Сортировка строк

- **Источник**: `determinism.sort.by` в YAML конфигурации
- **Применение**: Перед записью в CSV для воспроизводимости
- **Пример**:
  ```yaml
  determinism:
    sort:
      by: ["activity_chembl_id", "assay_chembl_id"]
      ascending: [True, True]
  ```

## 3. Стандарты нормализации

### DOI идентификаторы

- **Паттерн**: `^10\.\d+/[^\s]+$`
- **Нормализация**:
  - Удаление URL префиксов (`https://doi.org/`, `http://doi.org/`, `doi.org/`)
  - Приведение к нижнему регистру
  - Удаление пробелов

### ChEMBL ID

- **Паттерн**: `^CHEMBL\d+$`
- **Нормализация**:
  - Приведение к верхнему регистру
  - Удаление пробелов

### UniProt ID

- **Паттерн**: `^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$`
- **Нормализация**:
  - Приведение к верхнему регистру
  - Удаление пробелов

### PMID

- **Паттерн**: `^\d+$`
- **Нормализация**:
  - Удаление пробелов
  - Проверка на числовое значение

### InChI/InChI Key

- **InChI паттерн**: `^InChI=1S?/[^\s]+$`
- **InChI Key паттерн**: `^[A-Z]{14}-[A-Z]{10}-[A-Z]$`
- **Нормализация**:
  - InChI Key: приведение к верхнему регистру
  - Удаление пробелов

### Даты и время

- **Формат**: ISO 8601 `%Y-%m-%dT%H:%M:%SZ`
- **Нормализация**:
  - Приведение к UTC временной зоне
  - Форматирование в ISO 8601
  - Обработка naive datetime

### Булевы значения

- **True значения**: `true`, `1`, `yes`, `y`, `t`, `on`
- **False значения**: `false`, `0`, `no`, `n`, `f`, `off`
- **Нормализация**: Приведение к `bool` типу

### Числовые значения

- **Float**: Округление до 6 знаков после запятой
- **Int**: Приведение через `int(float(value))` для обработки "1.0"

## 4. Базовые схемы

### BaseNormalizedSchema

Общие системные поля для всех пайплайнов:

```python
class BaseNormalizedSchema(pa.DataFrameModel):
    index: Series[int] = pa.Field(ge=0, nullable=False)
    pipeline_version: Series[str] = pa.Field(nullable=False)
    source_system: Series[str] = pa.Field(nullable=False)
    extracted_at: Series[pd.Timestamp] = pa.Field(nullable=False)
    hash_row: Series[str] = pa.Field(checks=[pa.Check.str_matches(r'^[a-f0-9]{64}$')])
    hash_business_key: Series[str] = pa.Field(checks=[pa.Check.str_matches(r'^[a-f0-9]{64}$')])
    
    class Config:
        strict = True
        coerce = True
```

### BaseInputSchema

```python
class BaseInputSchema(pa.DataFrameModel):
    class Config:
        strict = False  # Разрешаем дополнительные колонки
        coerce = True
```

### BaseRawSchema

```python
class BaseRawSchema(pa.DataFrameModel):
    source: Series[str] = pa.Field(nullable=False)
    retrieved_at: Series[pd.Timestamp] = pa.Field(nullable=False)
    
    class Config:
        strict = False  # Разрешаем дополнительные поля от разных API
        coerce = True
```

## 5. Стандарты валидации

### Обязательные Checks

1. **ChEMBL ID**: `pa.Check.str_matches(r'^CHEMBL\d+$')`
2. **DOI**: `pa.Check.str_matches(r'^10\.\d+/[^\s]+$')`
3. **PMID**: `pa.Check.str_matches(r'^\d+$')`
4. **UniProt ID**: `pa.Check.str_matches(r'^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$')`
5. **InChI Key**: `pa.Check.str_matches(r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$')`
6. **SHA256 Hash**: `pa.Check.str_matches(r'^[a-f0-9]{64}$')`

### Диапазоны значений

1. **Молекулярная масса**: 50.0 - 2000.0 Da
2. **pChEMBL значение**: 3.0 - 12.0
3. **Стандартное значение активности**: 1e-12 - 1e-3
4. **Год публикации**: 1900 - 2030

## 6. Стандарты хеширования

### Hash Row

- **Назначение**: Уникальный идентификатор строки
- **Алгоритм**: SHA256
- **Исключения**: `hash_row`, `hash_business_key`
- **Формат**: `column1:value1|column2:value2|...`

### Hash Business Key

- **Назначение**: Уникальный идентификатор бизнес-объекта
- **Алгоритм**: SHA256
- **Колонки**: Определяются для каждой сущности
- **Формат**: `key1:value1|key2:value2|...`

## 7. Стандарты конфигурации

### Обязательные секции YAML

```yaml
pipeline:
  name: "entity_pipeline"
  version: "1.0.0"
  entity_type: "entity"

determinism:
  column_order: [...]
  sort:
    by: [...]
    ascending: [...]

io:
  output:
    csv:
      float_format: "%.6f"
      na_rep: ""
      line_terminator: "\n"

validation:
  strict: true
  coerce: true

normalization:
  functions: {...}
```

## 8. Приоритеты исправлений

### P1 (Критично - ломает сборку/данные)

1. Несоответствие `column_order`
2. Критичный `dtype_mismatch`
3. Отсутствующие обязательные поля
4. Провал Pandera Checks

### P2 (Влияет на воспроизводимость/совместимость)

1. Формат DOI/ID
2. `float_format`
3. Политика NA
4. Регистр имен
5. Сортировка

### P3 (Косметика и техдолг)

1. Алиасы
2. Описания
3. Комментарии в YAML
4. Рефакторинг Base*

## 9. Инструменты

### BaseNormalizer

```python
from library.common.base_normalizer import normalizer

# Нормализация DOI
doi = normalizer.normalize_doi("https://doi.org/10.1234/example")

# Нормализация ChEMBL ID
chembl_id = normalizer.normalize_chembl_id("chembl123")

# Вычисление хеша строки
hash_row = normalizer.compute_hash_row(row)
```

### Проверка соответствия

```python
from library.schemas.base_schema import BaseNormalizedSchema

# Валидация схемы
schema = BaseNormalizedSchema
validated_df = schema.validate(df)
```

## 10. Миграция

### Шаги применения стандартов

1. **Обновить YAML конфигурации** согласно патчам
2. **Применить Pandera схемы** с наследованием от BaseNormalizedSchema
3. **Интегрировать BaseNormalizer** в пайплайны
4. **Обновить логику записи** для детерминизма
5. **Провести валидацию** всех выходов

### Обратная совместимость

- Сохранение существующих API
- Постепенная миграция
- Валидация на каждом этапе
