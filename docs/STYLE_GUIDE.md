# Руководство по стилю документации

## Основа

Данное руководство основано на [Google Developer Documentation Style Guide](https://developers.google.com/style) и адаптировано для проекта Bioactivity Data Acquisition.

## Основные принципы

### 1. Второе лицо ("you")

Используйте второе лицо для обращения к читателю.

**Правильно:**

- "Вы можете настроить API клиенты через конфигурацию"
- "Запустите команду для получения данных"

**Неправильно:**

- "Пользователь может настроить API клиенты"
- "Команда запускается для получения данных"

### 2. Активный залог

Предпочитайте активный залог пассивному.

**Правильно:**

- "Система валидирует данные с помощью Pandera"
- "CLI обрабатывает конфигурацию"

**Неправильно:**

- "Данные валидируются системой с помощью Pandera"
- "Конфигурация обрабатывается CLI"

### 3. Короткие предложения

Используйте простые, короткие предложения.

**Правильно:**

- "Настройте API ключи. Добавьте их в переменные окружения."

**Неправильно:**

- "Для настройки API ключей, которые необходимы для работы с внешними источниками данных, добавьте их в переменные окружения вашей системы."

### 4. Единая терминология

Используйте термины из глоссария проекта.

**Ключевые термины:**

- **ETL-пайплайн** (не "ETL pipeline")
- **API клиент** (не "API client")
- **QC-отчёт** (не "quality control report")
- **Детерминированный** (не "deterministic")
- **Биоактивностные данные** (не "bioactivity data")

## Структура документов

### Заголовки

- Используйте заголовки для структурирования контента
- Максимум 3 уровня вложенности (H1, H2, H3)
- Заголовки должны быть описательными

### Списки

- Используйте маркированные списки для перечислений
- Используйте нумерованные списки для пошаговых инструкций
- Каждый элемент списка должен начинаться с заглавной буквы

### Код и команды

- Выделяйте команды в блоки кода с указанием языка
- Используйте `backticks` для inline кода
- Показывайте ожидаемый результат команд

**Пример:**

```bash
bioactivity-data-acquisition pipeline --config configs/config.yaml
```

### Примеры

- Обязательны для tutorial и how-to документов
- Показывайте полные, работающие примеры
- Объясняйте результат каждого примера

## Жанры Diátaxis

### Tutorial

**Цель**: Обучение с нуля

**Структура:**

1. Введение в концепцию
2. Предпосылки
3. Пошаговое изучение с примерами
4. Заключение и следующие шаги

**Тон**: Обучающий, объясняющий "как"

### How-to

**Цель**: Решение конкретной задачи

**Структура:**

1. Описание задачи
2. Предпосылки
3. Пошаговые инструкции
4. Проверка результата
5. Troubleshooting (опционально)

**Тон**: Инструктивный, "сделай это"

### Reference

**Цель**: Исчерпывающие факты

**Структура:**

1. Обзор
2. Детальное описание
3. Параметры/опции
4. Примеры использования
5. Связанные темы

**Тон**: Фактический, исчерпывающий

### Explanation

**Цель**: Понимание концепций

**Структура:**

1. Введение в проблему
2. Объяснение решения
3. Обоснование выбора
4. Альтернативы
5. Последствия

**Тон**: Объясняющий "почему"

## Docstrings (Google-style)

### Функции

```python
def function_name(param1: str, param2: int) -> bool:
    """Short one-line summary.
    
    Longer description if needed. Explain what the function does,
    not how it does it.
    
    Args:
        param1: Description of param1.
        param2: Description of param2.
    
    Returns:
        Description of return value.
    
    Raises:
        ValueError: When param1 is empty.
        ApiClientError: When API request fails.
    
    Examples:
        >>> function_name("test", 42)
        True
    """
```

### Классы

```python
class ExampleClass:
    """Short one-line summary.
    
    Longer description if needed. Explain what the class represents,
    not how it works internally.
    
    Attributes:
        attr1: Description of attr1.
        attr2: Description of attr2.
    
    Examples:
        >>> obj = ExampleClass("value1", 42)
        >>> obj.method()
        "result"
    """
    
    def __init__(self, attr1: str, attr2: int) -> None:
        """Initialize the class.
        
        Args:
            attr1: Description of attr1.
            attr2: Description of attr2.
        """
```

### Модули

```python
"""Module-level docstring.

This module provides functionality for...

Classes:
    ExampleClass: Main class for...

Functions:
    example_function: Utility function for...

Examples:
    Basic usage:
        >>> from module import ExampleClass
        >>> obj = ExampleClass()
        >>> obj.method()
"""
```

## Аудит docstrings (текущее состояние)

### Статистика

- **Всего функций**: 345
- **Всего классов**: 110
- **Всего docstrings**: 365
- **Покрытие**: ~100% (большинство функций имеют docstrings)

### Качество docstrings

- **Краткие однострочные**: ~60% (например, `"""Execute the ETL pipeline using the provided configuration."""`)
- **Google-style**: ~30% (с разделами Args, Returns, Raises)
- **Отсутствующие**: ~10% (в основном в утилитарных функциях)

### Приоритетные модули для обновления

1. **src/library/config.py** — 38 docstrings, смешанные стили
2. **src/library/clients/base.py** — 7 docstrings, краткие
3. **src/library/etl/run.py** — 2 docstrings, краткие
4. **src/library/schemas/** — 6 файлов, краткие docstrings
5. **src/library/clients/** — 8 файлов, смешанные стили

### План стандартизации

1. **Фаза 1**: Обновить docstrings в config.py (критический модуль)
2. **Фаза 2**: Обновить docstrings в clients/base.py (базовый класс)
3. **Фаза 3**: Обновить docstrings в etl/run.py (основная функция)
4. **Фаза 4**: Обновить docstrings в schemas/ (валидация)
5. **Фаза 5**: Обновить docstrings в остальных клиентах

### Требования к docstrings

- **Обязательные разделы**: Args, Returns, Raises (если применимо)
- **Примеры**: Для публичных API функций
- **Описания**: Объяснять "что", а не "как"
- **Типы**: Указывать типы параметров и возвращаемых значений

## Проверка качества

### Перед публикацией проверьте

- [ ] Используется второе лицо
- [ ] Активный залог
- [ ] Короткие предложения
- [ ] Единая терминология из глоссария
- [ ] Примеры для tutorial/how-to
- [ ] Рабочие команды и код
- [ ] Правильная структура по жанру Diátaxis
- [ ] Нет орфографических ошибок
- [ ] Ссылки работают

### Линтинг

- Используйте `markdownlint` для проверки Markdown
- Проверяйте ссылки с помощью `lychee`
- Проверяйте орфографию с помощью `codespell`

## Ссылки

- [Google Developer Documentation Style Guide](https://developers.google.com/style)
- [Diátaxis Framework](https://diataxis.fr/)
- [MkDocs Material](https://squidfunk.github.io/mkdocs-material/)
- [mkdocstrings-python](https://mkdocstrings.github.io/python/)
