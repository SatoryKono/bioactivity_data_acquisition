# Промпт: Добавить property-based тесты (P1-3)

## Контекст задачи

**Проблема:** Отсутствуют property-based тесты для критических трансформаций согласно ACCEPTANCE_CRITERIA.md раздел J.

**Статус:** ❌ **НЕ ИСПРАВЛЕНО** (P1-3)

**Ссылки:**

- `refactoring/ACCEPTANCE_CRITERIA.md` (строка 55): "Property-based тесты (Hypothesis) покрывают граничные случаи парсинга, нормализации и пагинации"
- `refactoring/AUDIT_REPORT_2025.md` (строка 991): "Найдено 1 упоминание в `test_json_utils.py`, но не сам Hypothesis"

## Существующие примеры property-based тестов

В проекте уже есть несколько примеров, которые можно использовать как референс:

1. **`tests/unit/utils/test_validation_properties.py`** — тесты для валидации схем
   - Использует `@given`, `@settings` из Hypothesis
   - Кастомные стратегии через `@st.composite`
   - Проверяет инварианты функции `_summarize_schema_errors`

2. **`tests/unit/normalizers/test_identifier_normalizer_properties.py`** — тесты для нормализации идентификаторов
   - Тестирует идемпотентность нормализации
   - Проверяет валидацию после нормализации
   - Использует стратегии для DOI, ORCID, OpenAlex ID, ChEMBL ID

3. **`tests/unit/sources/uniprot/test_idmapping_parser_properties.py`** — тесты для парсера UniProt ID mapping
   - Тестирует нормализацию различных вариантов payload'ов
   - Проверяет обработку "зашумленных" данных

4. **`tests/unit/core/test_api_client_properties.py`** — тесты для API клиента
   - Тестирует парсинг Retry-After header
   - Проверяет обработку числовых значений и timedelta

5. **`tests/unit/normalizers/test_string_normalizer_properties.py`** — тесты для нормализации строк
   - Тестирует обрезку пробелов
   - Проверяет обработку Unicode

6. **`tests/unit/sources/iuphar/test_pagination_properties.py`** — тесты для пагинации
   - Тестирует стратегии пагинации с различными параметрами

7. **`tests/unit/test_json_utils.py`** (строка 288) — упоминание Hypothesis для JSON records

## Требования из ACCEPTANCE_CRITERIA.md

**Раздел J. Тестовый контур:**
> "Property-based тесты (Hypothesis) покрывают граничные случаи парсинга, нормализации и пагинации; минимальные настройки (кол-во примеров/seed) зафиксированы."

**Раздел E. Пагинация:**
> "Отсутствие дубликатов и пропусков подтверждено e2e-тестами. Выполняются требования «этикета» API: обязательные заголовки и параметры клиента; проверяется контрактными тестами запросов."

## Критические компоненты для property-based тестирования

### 1. Нормализаторы (высокий приоритет)

**Местоположение:** `src/bioetl/normalizers/`

**Компоненты для тестирования:**

- `StringNormalizer` — обрезка пробелов, нормализация регистра (частично покрыто)
- `NumericNormalizer` — нормализация чисел, единицы измерения, precision
- `DateTimeNormalizer` — парсинг различных форматов дат, временные зоны
- `BooleanNormalizer` — нормализация булевых значений из строк/чисел
- `ChemistryNormalizer` — нормализация SMILES, InChI, молекулярных формул
- `IdentifierNormalizer` — нормализация DOI, PMID, ChEMBL ID, UniProt, PubChem CID (частично покрыто)

**Инварианты для проверки:**

- Идемпотентность: `normalize(normalize(x)) == normalize(x)`
- Детерминизм: два вызова с одинаковым входом дают одинаковый результат
- Типы: результат имеет ожидаемый тип
- Валидация: если результат не None, то он проходит validate()

**Пример стратегии:**

```python
from hypothesis import given, settings
from hypothesis import strategies as st

@given(st.text(min_size=0, max_size=100))
@settings(max_examples=500)
def test_string_normalize_is_idempotent(value: str) -> None:
    normalizer = StringNormalizer()
    once = normalizer.normalize(value)
    if once is not None:
        twice = normalizer.normalize(once)
        assert twice == once
```

### 2. Парсеры (высокий приоритет)

**Местоположение:** `src/bioetl/sources/<source>/parser/`

**Компоненты для тестирования:**

- Парсеры ответов API для каждого источника
- Обработка различных форматов JSON/XML
- Парсинг частичных/поврежденных данных
- Обработка edge cases (пустые массивы, null значения, неожиданные типы)

**Инварианты для проверки:**

- Чистые функции (без side effects)
- Обработка всех допустимых форматов ответа API
- Graceful degradation при неожиданных полях
- Сохранение всех полей из исходного ответа

**Пример стратегии для JSON парсера:**

```python
@st.composite
def api_response_strategy(draw):
    """Generate valid API response structures."""
    return draw(st.one_of(
        st.dictionaries(st.text(), st.one_of(st.text(), st.integers(), st.floats(), st.booleans(), st.none())),
        st.lists(st.dictionaries(st.text(), st.one_of(st.text(), st.integers(), st.floats()))),
    ))

@given(api_response_strategy())
@settings(max_examples=200)
def test_parser_handles_various_json_structures(payload: dict | list) -> None:
    parser = SomeSourceParser()
    result = parser.parse(payload)
    # Проверка инвариантов
    assert result is not None or payload is None
    # Если результат словарь, проверяем что все ключи строки
    if isinstance(result, dict):
        assert all(isinstance(k, str) for k in result.keys())
```

### 3. Пагинация (средний приоритет)

**Местоположение:** `src/bioetl/sources/<source>/pagination/`

**Компоненты для тестирования:**

- Стратегии пагинации: PageNumber, Cursor, OffsetLimit, Token
- Отсутствие дубликатов и пропусков
- Корректная обработка последней страницы

**Инварианты для проверки:**

- Все записи уникальны (нет дубликатов)
- Порядок стабилен
- Нет пропусков между страницами
- Корректная обработка пустых результатов

**Существующий пример:** `tests/unit/sources/iuphar/test_pagination_properties.py`

### 4. Валидация схем Pandera (средний приоритет)

**Местоположение:** `src/bioetl/schemas/`, `tests/schemas/`

**Компоненты для тестирования:**

- Генерация валидных данных по схеме
- Проверка что нормализованные данные проходят валидацию
- Обработка edge cases (null, пустые строки, граничные значения)

**Существующий пример:** `tests/unit/utils/test_validation_properties.py`

### 5. Трансформации данных (средний приоритет)

**Местоположение:** `src/bioetl/sources/<source>/normalizer/`

**Компоненты для тестирования:**

- Трансформации между форматами (например, ChEMBL → UnifiedSchema)
- Сохранение всех полей (extras)
- Приведение единиц измерения
- Канонизация идентификаторов

## Структура файлов для новых тестов

Рекомендуемая структура (следуя существующим примерам):

```
tests/
├── unit/
│   ├── normalizers/
│   │   ├── test_numeric_normalizer_properties.py      # для NumericNormalizer
│   │   ├── test_datetime_normalizer_properties.py     # для DateTimeNormalizer
│   │   ├── test_boolean_normalizer_properties.py       # для BooleanNormalizer
│   │   └── test_chemistry_normalizer_properties.py     # для ChemistryNormalizer
│   ├── sources/
│   │   ├── <source>/
│   │   │   ├── test_<source>_parser_properties.py      # для парсера источника
│   │   │   └── test_<source>_normalizer_properties.py  # для нормализатора источника
│   └── utils/
│       └── test_<utility>_properties.py                # для утилит
```

## Требования к реализации

### 1. Установка зависимостей

Убедиться что `hypothesis` добавлен в `requirements.txt`:

```txt
hypothesis>=6.0.0
```

### 2. Структура теста

Все property-based тесты должны:

- Использовать `pytest.importorskip("hypothesis")` для graceful skip если библиотека отсутствует
- Иметь `@settings` с фиксированным `max_examples` (рекомендуется 200-500)
- Использовать `@given` с явными стратегиями
- Тестировать инварианты, а не конкретные значения
- Иметь docstring с описанием проверяемого инварианта

**Шаблон:**

```python
"""Property-based tests for <component>."""

from __future__ import annotations

import pytest
pytest.importorskip("hypothesis")

from hypothesis import given, settings
from hypothesis import strategies as st

from bioetl.<module> import <Component>

@given(<strategy>())
@settings(max_examples=200, deadline=None)
def test_<component>_property(<parameter>):
    """<Component> should satisfy <invariant>."""
    component = <Component>()
    # Проверка инварианта
    assert <invariant_check>
```

### 3. Кастомные стратегии

Для сложных данных использовать `@st.composite`:

```python
@st.composite
def <component>_inputs(draw: st.DrawFn) -> <Type>:
    """Generate diverse test inputs for <component>."""
    # Логика генерации
    return <generated_value>
```

### 4. Приоритеты реализации

**Приоритет 1 (критично):**

- `NumericNormalizer` — нормализация чисел, единицы
- `DateTimeNormalizer` — парсинг дат
- `ChemistryNormalizer` — SMILES, InChI

**Приоритет 2 (важно):**

- Парсеры для источников без property-based тестов
- `BooleanNormalizer`
- Трансформации в normalizer'ах источников

**Приоритет 3 (желательно):**

- Расширение существующих property-based тестов
- Пагинация (дополнить существующие)

### 5. Примеры инвариантов для тестирования

**Идемпотентность:**

```python
result = f(x)
assert f(result) == result
```

**Детерминизм:**

```python
result1 = f(x)
result2 = f(x)
assert result1 == result2
```

**Соответствие типам:**

```python
result = f(x)
assert isinstance(result, ExpectedType) or result is None
```

**Валидация:**

```python
result = f(x)
if result is not None:
    assert validator.validate(result)
```

**Отсутствие потерь данных:**

```python
original = extract_all_fields(x)
transformed = transform(x)
normalized = normalize(transformed)

# Проверка что все исходные поля сохранены в extras или явных полях

assert all(field in normalized for field in original)
```

### 6. Интеграция с CI

Тесты должны:

- Запускаться в CI как часть `pytest` suite
- Иметь фиксированный seed для воспроизводимости (опционально через pytest.ini)
- Быть помечены маркером `@pytest.mark.property` если нужна отдельная категория

**Пример pytest.ini:**

```ini
[pytest]
markers =
    property: property-based tests using Hypothesis
```

### 7. Документация

Для каждого нового property-based теста:

- Добавить docstring с описанием проверяемого инварианта
- Указать минимальные настройки в комментариях
- Задокументировать стратегии генерации данных

## Критерии завершения

- ✅ Property-based тесты для всех критических нормализаторов (Numeric, DateTime, Chemistry, Boolean)
- ✅ Property-based тесты для парсеров основных источников (ChEMBL, PubChem, UniProt)
- ✅ Тесты используют Hypothesis с фиксированными настройками
- ✅ Тесты проверяют инварианты, а не конкретные значения
- ✅ Все тесты проходят в CI
- ✅ Документация обновлена (если требуется)

## Ссылки на существующие примеры

- `tests/unit/utils/test_validation_properties.py` — валидация схем
- `tests/unit/normalizers/test_identifier_normalizer_properties.py` — нормализация идентификаторов
- `tests/unit/sources/uniprot/test_idmapping_parser_properties.py` — парсинг UniProt
- `tests/unit/core/test_api_client_properties.py` — API клиент
- `tests/unit/normalizers/test_string_normalizer_properties.py` — нормализация строк
- `tests/unit/sources/iuphar/test_pagination_properties.py` — пагинация

## Примечания

- При отсутствии Hypothesis тесты должны gracefully skip через `pytest.importorskip`
- Использовать фиксированные стратегии для воспроизводимости
- Фокусироваться на критических трансформациях согласно ACCEPTANCE_CRITERIA.md
