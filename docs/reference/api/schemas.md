# Схемы данных

Документация по схемам валидации данных с использованием Pandera.

## Основные схемы

### Входные схемы

Схемы для валидации входных данных.

::: library.schemas.input_schema

### Выходные схемы

Схемы для валидации выходных данных.

::: library.schemas.output_schema

### Схемы тестовых данных

Схемы для валидации данных молекул.

::: library.schemas.testitem_schema

### Схемы мишеней

Схемы для валидации данных мишеней.

::: library.schemas.target_schema

### Схемы экспериментов

Схемы для валидации данных экспериментов.

::: library.schemas.assay_schema

## Основные классы схем

### Базовые схемы

- `BaseSchema`: Базовая схема с общими полями
- `InputSchema`: Схема для входных данных
- `OutputSchema`: Схема для выходных данных

### Специализированные схемы

- `TestitemSchema`: Схема для данных молекул
- `TargetSchema`: Схема для данных мишеней
- `ActivitySchema`: Схема для данных активностей
- `AssaySchema`: Схема для данных экспериментов

## Примеры использования

### Валидация входных данных

```python
from library.schemas.input_schema import TestitemInputSchema
import pandera as pa

schema = TestitemInputSchema()
df = pd.read_csv("input/testitem.csv")

try:
    validated_df = schema.validate(df)
    print("Данные валидны")
except pa.errors.SchemaError as e:
    print(f"Ошибка валидации: {e}")
```

### Валидация выходных данных

```python
from library.schemas.output_schema import TestitemOutputSchema

schema = TestitemOutputSchema()
result_df = process_data(input_df)

validated_result = schema.validate(result_df)
```

### Кастомная валидация

```python
import pandera as pa
from pandera import Column, DataFrameSchema

custom_schema = DataFrameSchema({
    "molecule_id": Column(str, pa.Check.str_length(min_length=1)),
    "activity_value": Column(float, pa.Check.greater_than(0)),
    "activity_type": Column(str, pa.Check.isin(["IC50", "EC50", "Ki"]))
})
```

## Настройка валидации

### Строгая валидация

```python
schema = TestitemSchema(strict=True)
validated_df = schema.validate(df, lazy=True)
```

### Ленивая валидация

```python
schema = TestitemSchema()
try:
    validated_df = schema.validate(df, lazy=True)
except pa.errors.SchemaErrors as e:
    print(f"Найдены ошибки: {e.failure_cases}")
```

### Частичная валидация

```python
schema = TestitemSchema()
validated_df = schema.validate(df, subset=["molecule_id", "activity_value"])
```

## Обработка ошибок валидации

### Обработка SchemaError

```python
try:
    validated_df = schema.validate(df)
except pa.errors.SchemaError as e:
    logger.error(f"Ошибка валидации схемы: {e}")
    # Обработка ошибки
```

### Обработка SchemaErrors

```python
try:
    validated_df = schema.validate(df, lazy=True)
except pa.errors.SchemaErrors as e:
    logger.error(f"Множественные ошибки валидации: {e}")
    for error in e.failure_cases:
        logger.error(f"Строка {error.index}: {error.check_output}")
```

## Расширение схем

### Создание производной схемы

```python
from library.schemas.testitem_schema import TestitemSchema
import pandera as pa

class ExtendedTestitemSchema(TestitemSchema):
    additional_field = Column(str, nullable=True)
    
    @pa.check("activity_value")
    def activity_value_positive(cls, series):
        return series > 0
```

### Добавление кастомных проверок

```python
from library.schemas.target_schema import TargetSchema
import pandera as pa

class ValidatedTargetSchema(TargetSchema):
    @pa.check("target_type")
    def validate_target_type(cls, series):
        valid_types = ["protein", "nucleic_acid", "small_molecule"]
        return series.isin(valid_types)
```

## Производительность валидации

### Оптимизация для больших датасетов

```python
# Валидация по частям
chunk_size = 10000
for chunk in pd.read_csv("large_file.csv", chunksize=chunk_size):
    validated_chunk = schema.validate(chunk)
    # Обработка валидного чанка
```

### Кэширование схем

```python
from functools import lru_cache

@lru_cache(maxsize=1)
def get_cached_schema():
    return TestitemSchema()

schema = get_cached_schema()
```

## Отладка схем

### Включение отладочной информации

```python
import logging
logging.basicConfig(level=logging.DEBUG)

schema = TestitemSchema()
validated_df = schema.validate(df)
```

### Анализ ошибок валидации

```python
try:
    validated_df = schema.validate(df, lazy=True)
except pa.errors.SchemaErrors as e:
    # Анализ типов ошибок
    error_summary = e.failure_cases.groupby("check").size()
    print("Распределение ошибок по типам:")
    print(error_summary)
```
