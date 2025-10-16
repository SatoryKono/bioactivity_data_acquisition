# Форматирование литературных ссылок

## Обзор

Модуль `library.tools.citation*formatter`предоставляет функциональность для
автоматического формирования литературных ссылок на научные статьи по
установленным правилам.

## Основные функции

### `format*citation(journal, volume, issue, first*page, last*page) -> str`

Формирует литературную ссылку на статью по заданным параметрам.

## Параметры

-`journal`(str): Название журнала

-`volume`(str): Том журнала

-`issue`(str): Номер выпуска

-`first*page`(str/int): Первая страница

-`last*page`(str/int): Последняя страница

## Возвращает

- Форматированную литературную ссылку (str)

## Правила форматирования

#### Базовый формат

- С issue:`"<journal>, <volume> (<issue>). <pages>"`

- Без issue:`"<journal>, <volume>. <pages>"`

- Без volume:`"<journal>. <pages>"`

#### Правила страниц

1. **Диапазон страниц**(`p. X-Y`):

   - Используется только если ВСЕ условия выполнены:

     - `first*page`и`last*page`— целые числа

     -`100000 > last*page > first*page`

     -`last*page ≠ first*page`

2.**Одна страница**(`X`):

- Если `last*page == first*page`

- Если`last*page >= 100000`

- Если`last*page`не число

- Если`first*page`не число (возвращается как есть, например`e221234`)

3.**Отсутствие страниц**:

- Если оба поля пусты, раздел `<pages>`опускается

#### Обработка данных

- Все поля обрезаются от пробелов

-`journal`, `volume`, `issue`хранятся как строки

-`first*page`и`last*page`приводятся к`int`если возможно

- Если`first*page`отсутствует, а`last*page`присутствует → используется`last*page`как единичная страница

- Двойные пробелы удаляются

- Висячие знаки препинания очищаются

### `add*citation*column(df) -> pd.DataFrame`

Добавляет колонку`document*citation`к DataFrame с документами.

## Параметры

-`df`(pd.DataFrame): DataFrame с колонками`journal`, `volume`, `issue`, `first*page`, `last*page`##
Возвращает:

- DataFrame с добавленной колонкой`document*citation`

## Примеры использования

### Пример 1: Стандартная ссылка с issue

```

from library.tools.citation*formatter import format*citation

citation = format*citation(
    journal="Nature",
    volume="612",
    issue="7940",
    first*page="100",
    last*page="105"
)

## Результат: "Nature, 612 (7940). p. 100-105"

```

### Пример 2: Без issue

```

citation = format*citation(
    journal="Nature",
    volume="612",
    issue="",
    first*page="100",
    last*page="100"
)

## Результат: "Nature, 612. 100"

```

### Пример 3: Нечисловая страница

```

citation = format*citation(
    journal="JAMA",
    volume="327",
    issue="12",
    first*page="e221234",
    last*page="e221234"
)

## Результат: "JAMA, 327 (12). e221234"

```

### Пример 4: Большой last*page (аномалия)

```

citation = format*citation(
    journal="Cell",
    volume="",
    issue="",
    first*page="50",
    last*page="200000"
)

## Результат: "Cell. 50"

```

### Пример 5: Работа с DataFrame

```

import pandas as pd
from library.tools.citation*formatter import add*citation*column

df = pd.DataFrame({
    'journal': ['Nature', 'Science'],
    'volume': ['612', '380'],
    'issue': ['7940', ''],
    'first*page': ['100', '50'],
    'last*page': ['105', '60']
})

df*with*citations = add*citation*column(df)

## Добавляет колонку 'document*citation' с форматированными ссылками

```

## Интеграция с pipeline

Функция автоматически вызывается при сохранении документов через`write*document*outputs()`.

Колонка `document*citation`добавляется к итоговому DataFrame перед записью в
CSV:

- Находится в`data/output/documents*<date*tag>.csv`

- Включена в схему`DocumentOutputSchema`

- Nullable (может быть пустой)

## Тестирование

Запустите демонстрационный скрипт:

```

python scripts/test*citation*demo.py

```

Или используйте pytest (требуется настройка окружения):

```

pytest tests/test*citation*formatter.py -v

```

## Схема данных

Поле`document_citation`добавлено в схему выходных данных:

- **Тип**:`Series[str]`

- **Nullable**:`True`

- **Описание**: "Formatted citation string"

## История изменений

- **2025-10-15**: Первоначальная реализация функциональности формирования литературных ссылок
