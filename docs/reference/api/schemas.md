# Схемы данных

Pandera схемы для валидации и нормализации данных.

## Основные схемы

### Входные данные
:::: library.schemas.input_schema.RawBioactivitySchema

### Нормализованные данные
:::: library.schemas.output_schema.NormalizedBioactivitySchema

## Схемы документов

### Входные документы
:::: library.schemas.document_schema.DocumentInputSchema

### Выходные документы
:::: library.schemas.document_schema.DocumentOutputSchema

## Примеры использования

### Валидация данных

```python
from library.schemas import RawBioactivitySchema, NormalizedBioactivitySchema
import pandas as pd

# Создание схемы
schema = RawBioactivitySchema.to_schema()

# Валидация DataFrame
validated_data = schema.validate(df)

# Создание пустого DataFrame
empty_df = schema.empty_dataframe()
```

### Проверка схемы

```python
# Проверка без исключений
try:
    validated_data = schema.validate(df, lazy=True)
except ValidationError as e:
    print(f"Validation failed: {e}")
```

### Создание схемы из конфигурации

```python
from library.schemas import create_schema_from_config

# Создание схемы на основе конфигурации
schema = create_schema_from_config(config.schemas)
```
