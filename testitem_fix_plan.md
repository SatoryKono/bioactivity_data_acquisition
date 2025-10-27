# План исправления ошибок testitem скрипта

## Анализ проблем

### 1. Критические ошибки валидации данных
- **first_approval**: ожидается строка, получено число (1950)
- **nstereo**: ожидается int64, получен object

### 2. Ошибки логирования
- Множественные `TypeError: not all arguments converted during string formatting`
- Проблемы с форматированием строк в логгере

## План исправления

### Этап 1: Исправление ошибок валидации данных (Критично)

#### 1.1 Исправление типа данных first_approval
**Файл:** `src/library/testitem/normalize.py`
**Проблема:** Поле содержит год как число, но схема ожидает ISO 8601 строку
**Решение:**
```python
# Добавить конвертацию года в ISO 8601 дату
if 'first_approval' in normalized_df.columns:
    normalized_df['first_approval'] = normalized_df['first_approval'].apply(
        lambda x: f"{x}-01-01T00:00:00Z" if pd.notna(x) and isinstance(x, (int, float)) else x
    )
```

#### 1.2 Исправление типа данных nstereo
**Файл:** `src/library/testitem/normalize.py`
**Проблема:** Поле имеет тип object вместо int64
**Решение:**
```python
# Принудительная конвертация в int64 с обработкой NaN
if 'nstereo' in normalized_df.columns:
    normalized_df['nstereo'] = pd.to_numeric(normalized_df['nstereo'], errors='coerce').astype('Int64')
```

#### 1.3 Обновление схемы валидации
**Файл:** `src/library/testitem/schemas.py`
**Решение:** Добавить более гибкие правила валидации для проблемных полей

### Этап 2: Исправление ошибок логирования (Важно)

#### 2.1 Исправление форматирования строк в validate.py
**Файл:** `src/library/testitem/validate.py`
**Проблема:** Строка 303, 331, 342
**Решение:**
```python
# Заменить
logger.info("Validating input testitem data: %d records", len(df))
# На
logger.info("Validating input testitem data: %d records", len(df))

# Или использовать f-strings
logger.info(f"Validating input testitem data: {len(df)} records")
```

#### 2.2 Исправление форматирования в normalize.py
**Файл:** `src/library/testitem/normalize.py`
**Проблема:** Строки 49, 72
**Решение:**
```python
# Заменить проблемные строки на правильное форматирование
logger.info("Normalizing %d testitem records", len(df))
logger.info("Normalization completed. Output: %d records", len(normalized_df))
```

### Этап 3: Улучшение обработки данных (Рекомендуется)

#### 3.1 Добавление проверок типов данных
**Файл:** `src/library/testitem/normalize.py`
**Решение:**
```python
def _ensure_correct_types(self, df: pd.DataFrame) -> pd.DataFrame:
    """Обеспечивает правильные типы данных перед валидацией"""
    # Конвертация дат
    date_columns = ['first_approval', 'extracted_at']
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # Конвертация числовых полей
    int_columns = ['nstereo', 'molregno', 'parent_molregno']
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    return df
```

#### 3.2 Улучшение обработки ошибок валидации
**Файл:** `src/library/testitem/validate.py`
**Решение:**
```python
def validate_normalized(self, data: pd.DataFrame) -> pd.DataFrame:
    try:
        # Применяем исправления типов перед валидацией
        data = self._ensure_correct_types(data)
        validated_df = schema.validate(data, lazy=True)
        return validated_df
    except SchemaErrors as exc:
        logger.error("Validation failed, attempting data type fixes...")
        # Попытка исправить типы данных
        data = self._fix_data_types(data)
        validated_df = schema.validate(data, lazy=True)
        return validated_df
```

### Этап 4: Тестирование и проверка

#### 4.1 Создание тестовых данных
**Файл:** `test_data/testitem_sample.csv`
**Содержимое:**
```csv
molecule_chembl_id,first_approval,nstereo
CHEMBL25,1950-01-01T00:00:00Z,2
CHEMBL50,1960-01-01T00:00:00Z,1
```

#### 4.2 Пошаговое тестирование
1. Запуск с тестовыми данными
2. Проверка каждого этапа (extract → normalize → validate)
3. Проверка выходных файлов
4. Анализ качества данных

## Приоритеты выполнения

### Высокий приоритет (Критично)
1. ✅ Исправление типов данных first_approval и nstereo
2. ✅ Исправление ошибок логирования
3. ✅ Обновление схемы валидации

### Средний приоритет (Важно)
4. ✅ Улучшение обработки ошибок
5. ✅ Добавление проверок типов данных

### Низкий приоритет (Рекомендуется)
6. ✅ Создание тестовых данных
7. ✅ Документирование изменений

## Ожидаемые результаты

После исправления:
- ✅ Скрипт должен успешно обработать все 5 записей
- ✅ Создать выходные файлы в `data/output/testitem/`
- ✅ Сгенерировать QC отчеты
- ✅ Выполнить анализ колонок (пустые/заполненные)
- ✅ Устранить ошибки логирования

## Время выполнения
- **Этап 1-2:** 2-3 часа (критические исправления)
- **Этап 3:** 1-2 часа (улучшения)
- **Этап 4:** 1 час (тестирование)
- **Общее время:** 4-6 часов

---
*План создан: 2025-10-26*
*Статус: Готов к выполнению*
