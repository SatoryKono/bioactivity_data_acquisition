# Форматирование литературных ссылок

## Обзор

Модуль `library.tools.citation_formatter` предоставляет функциональность для автоматического формирования литературных ссылок на научные статьи по установленным правилам.

## Основные функции

### `format_citation(journal, volume, issue, first_page, last_page) -> str`

Формирует литературную ссылку на статью по заданным параметрам.

**Параметры:**
- `journal` (str): Название журнала
- `volume` (str): Том журнала
- `issue` (str): Номер выпуска
- `first_page` (str/int): Первая страница
- `last_page` (str/int): Последняя страница

**Возвращает:**
- Форматированную литературную ссылку (str)

**Правила форматирования:**

#### Базовый формат:
- С issue: `"<journal>, <volume> (<issue>). <pages>"`
- Без issue: `"<journal>, <volume>. <pages>"`
- Без volume: `"<journal>. <pages>"`

#### Правила страниц:

1. **Диапазон страниц** (`p. X-Y`):
   - Используется только если ВСЕ условия выполнены:
     - `first_page` и `last_page` — целые числа
     - `100000 > last_page > first_page`
     - `last_page ≠ first_page`

2. **Одна страница** (`X`):
   - Если `last_page == first_page`
   - Если `last_page >= 100000`
   - Если `last_page` не число
   - Если `first_page` не число (возвращается как есть, например `e221234`)

3. **Отсутствие страниц**:
   - Если оба поля пусты, раздел `<pages>` опускается

#### Обработка данных:
- Все поля обрезаются от пробелов
- `journal`, `volume`, `issue` хранятся как строки
- `first_page` и `last_page` приводятся к `int` если возможно
- Если `first_page` отсутствует, а `last_page` присутствует → используется `last_page` как единичная страница
- Двойные пробелы удаляются
- Висячие знаки препинания очищаются

### `add_citation_column(df) -> pd.DataFrame`

Добавляет колонку `citation` к DataFrame с документами.

**Параметры:**
- `df` (pd.DataFrame): DataFrame с колонками `journal`, `volume`, `issue`, `first_page`, `last_page`

**Возвращает:**
- DataFrame с добавленной колонкой `citation`

## Примеры использования

### Пример 1: Стандартная ссылка с issue
```python
from library.tools.citation_formatter import format_citation

citation = format_citation(
    journal="Nature",
    volume="612",
    issue="7940",
    first_page="100",
    last_page="105"
)
# Результат: "Nature, 612 (7940). p. 100-105"
```

### Пример 2: Без issue
```python
citation = format_citation(
    journal="Nature",
    volume="612",
    issue="",
    first_page="100",
    last_page="100"
)
# Результат: "Nature, 612. 100"
```

### Пример 3: Нечисловая страница
```python
citation = format_citation(
    journal="JAMA",
    volume="327",
    issue="12",
    first_page="e221234",
    last_page="e221234"
)
# Результат: "JAMA, 327 (12). e221234"
```

### Пример 4: Большой last_page (аномалия)
```python
citation = format_citation(
    journal="Cell",
    volume="",
    issue="",
    first_page="50",
    last_page="200000"
)
# Результат: "Cell. 50"
```

### Пример 5: Работа с DataFrame
```python
import pandas as pd
from library.tools.citation_formatter import add_citation_column

df = pd.DataFrame({
    'journal': ['Nature', 'Science'],
    'volume': ['612', '380'],
    'issue': ['7940', ''],
    'first_page': ['100', '50'],
    'last_page': ['105', '60']
})

df_with_citations = add_citation_column(df)
# Добавляет колонку 'citation' с форматированными ссылками
```

## Интеграция с pipeline

Функция автоматически вызывается при сохранении документов через `write_document_outputs()`.

Колонка `citation` добавляется к итоговому DataFrame перед записью в CSV:
- Находится в `data/output/documents_<date_tag>.csv`
- Включена в схему `DocumentOutputSchema`
- Nullable (может быть пустой)

## Тестирование

Запустите демонстрационный скрипт:
```bash
python scripts/test_citation_demo.py
```

Или используйте pytest (требуется настройка окружения):
```bash
pytest tests/test_citation_formatter.py -v
```

## Схема данных

Поле `citation` добавлено в схему выходных данных:
- **Тип**: `Series[str]`
- **Nullable**: `True`
- **Описание**: "Formatted citation string"

## История изменений

- **2025-10-15**: Первоначальная реализация функциональности формирования литературных ссылок
